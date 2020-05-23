package com.softwareverde.bitcoin.server.module.node.sync;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.validator.BlockHeaderValidator;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.context.BlockHeaderValidatorContext;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.manager.BitcoinNodeManager;
import com.softwareverde.concurrent.pool.ThreadPool;
import com.softwareverde.concurrent.service.SleepyService;
import com.softwareverde.constable.list.List;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.util.TransactionUtil;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.time.NetworkTime;
import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.util.Container;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MilliTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.util.concurrent.atomic.AtomicBoolean;

public class BlockHeaderDownloader extends SleepyService {
    public static final Long MAX_TIMEOUT_MS = (15L * 1000L); // 15 Seconds...

    protected final SystemTime _systemTime = new SystemTime();
    protected final DatabaseManagerFactory _databaseManagerFactory;
    protected final NetworkTime _networkTime;
    protected final BitcoinNodeManager _nodeManager;
    protected final BlockDownloadRequester _blockDownloadRequester;
    protected final ThreadPool _threadPool;
    protected final MilliTimer _timer;
    protected final BitcoinNodeManager.DownloadBlockHeadersCallback _downloadBlockHeadersCallback;
    protected final Container<Float> _averageBlockHeadersPerSecond = new Container<Float>(0F);

    protected final Object _headersDownloadedPin = new Object();
    protected final AtomicBoolean _isProcessingHeaders = new AtomicBoolean(false);
    protected final Object _genesisBlockPin = new Object();
    protected Boolean _hasGenesisBlock = false;

    protected Integer _maxHeaderBatchSize = 2000;

    protected Long _headBlockHeight = 0L;
    protected Sha256Hash _lastBlockHash = BlockHeader.GENESIS_BLOCK_HASH;
    protected BlockHeader _lastBlockHeader = null;
    protected Long _minBlockTimestamp = (_systemTime.getCurrentTimeInSeconds() - 3600L); // Default to an hour ago...
    protected Long _blockHeaderCount = 0L;

    protected Runnable _newBlockHeaderAvailableCallback = null;

    protected Boolean _checkForGenesisBlockHeader() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final Sha256Hash lastKnownHash = blockHeaderDatabaseManager.getHeadBlockHeaderHash();

            synchronized (_genesisBlockPin) {
                _hasGenesisBlock = (lastKnownHash != null);
                _genesisBlockPin.notifyAll();
            }

            return _hasGenesisBlock;
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return false;
        }
    }

    protected void _downloadGenesisBlock() {
        final Runnable retry = new Runnable() {
            @Override
            public void run() {
                try { Thread.sleep(5000L); } catch (final InterruptedException exception) { return; }
                _downloadGenesisBlock();
            }
        };

        _nodeManager.requestBlock(Block.GENESIS_BLOCK_HASH, new BitcoinNodeManager.DownloadBlockCallback() {
            @Override
            public void onResult(final Block block) {
                final Sha256Hash blockHash = block.getHash();
                Logger.trace("GENESIS RECEIVED: " + blockHash);
                if (_checkForGenesisBlockHeader()) { return; } // NOTE: This can happen if the BlockDownloader received the GenesisBlock first...

                boolean genesisBlockWasStored = false;
                try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
                    genesisBlockWasStored = _validateAndStoreBlockHeader(block, _headBlockHeight, databaseManager);
                }
                catch (final DatabaseException databaseException) {
                    Logger.debug(databaseException);
                }
                if (! genesisBlockWasStored) {
                    _threadPool.execute(retry);
                    return;
                }

                Logger.trace("GENESIS STORED: " + block.getHash());

                synchronized (_genesisBlockPin) {
                    _hasGenesisBlock = true;
                    _genesisBlockPin.notifyAll();
                }
            }

            @Override
            public void onFailure(final Sha256Hash blockHash) {
                _threadPool.execute(retry);
            }
        });
    }

    protected List<BlockId> _insertBlockHeaders(final List<BlockHeader> blockHeaders, final BlockHeaderDatabaseManager blockHeaderDatabaseManager) {
        try {
            return blockHeaderDatabaseManager.insertBlockHeaders(blockHeaders, _maxHeaderBatchSize);
        }
        catch (final DatabaseException exception) {
            Logger.debug(exception);
            return null;
        }
    }

    protected Boolean _validateAndStoreBlockHeader(final BlockHeader blockHeader, final Long blockHeight, final DatabaseManager databaseManager) throws DatabaseException {
        final Sha256Hash blockHash = blockHeader.getHash();

        if (! blockHeader.isValid()) {
            Logger.info("Invalid BlockHeader: " + blockHash);
            return false;
        }

        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
        final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

        synchronized (BlockHeaderDatabaseManager.MUTEX) {
            TransactionUtil.startTransaction(databaseConnection);
            final BlockId blockId = blockHeaderDatabaseManager.storeBlockHeader(blockHeader);

            if (blockId == null) {
                Logger.info("Error storing BlockHeader: " + blockHash);
                TransactionUtil.rollbackTransaction(databaseConnection);
                return false;
            }

            final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(blockId);
            final BlockHeaderValidatorContext blockHeaderValidatorContext = new BlockHeaderValidatorContext(blockchainSegmentId, databaseManager, _networkTime);
            final BlockHeaderValidator<?> blockHeaderValidator = new BlockHeaderValidator<>(blockHeaderValidatorContext);

            final BlockHeaderValidator.BlockHeaderValidationResult blockHeaderValidationResult = blockHeaderValidator.validateBlockHeader(blockHeader, blockHeight);
            if (! blockHeaderValidationResult.isValid) {
                Logger.info("Invalid BlockHeader: " + blockHeaderValidationResult.errorMessage + " (" + blockHash + ")");
                TransactionUtil.rollbackTransaction(databaseConnection);
                return false;
            }

            _headBlockHeight = Math.max(blockHeight, _headBlockHeight);

            TransactionUtil.commitTransaction(databaseConnection);
        }

        return true;
    }

    protected Boolean _validateAndStoreBlockHeaders(final List<BlockHeader> blockHeaders, final DatabaseManager databaseManager) throws DatabaseException {
        if (blockHeaders.isEmpty()) { return true; }

        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
        final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

        synchronized (BlockHeaderDatabaseManager.MUTEX) {
            { // Validate blockHeaders are sequential...
                final BlockHeader firstBlockHeader = blockHeaders.get(0);
                if (! firstBlockHeader.isValid()) { return false; }

                final BlockId previousBlockId = blockHeaderDatabaseManager.getBlockHeaderId(firstBlockHeader.getPreviousBlockHash());
                final boolean previousBlockExists = (previousBlockId != null);
                if (! previousBlockExists) {
                    final Boolean isGenesisBlock = Util.areEqual(BlockHeader.GENESIS_BLOCK_HASH, firstBlockHeader.getHash());
                    if (! isGenesisBlock) { return false; }
                }
                else {
                    final Boolean isContentiousBlock = blockHeaderDatabaseManager.hasChildBlock(previousBlockId);
                    if (isContentiousBlock) {
                        // BlockHeaders cannot be batched due to potential forks...
                        long blockHeight = (blockHeaderDatabaseManager.getBlockHeight(previousBlockId) + 1L);
                        for (final BlockHeader blockHeader : blockHeaders) {
                            final Boolean isValid = _validateAndStoreBlockHeader(blockHeader, blockHeight, databaseManager);
                            if (! isValid) { return false; }
                            blockHeight += 1L;
                        }
                        return true;
                    }
                }
                Sha256Hash previousBlockHash = firstBlockHeader.getPreviousBlockHash();
                for (final BlockHeader blockHeader : blockHeaders) {
                    if (! blockHeader.isValid()) { return false; }
                    if (! Util.areEqual(previousBlockHash, blockHeader.getPreviousBlockHash())) {
                        return false;
                    }
                    previousBlockHash = blockHeader.getHash();
                }
            }

            TransactionUtil.startTransaction(databaseConnection);

            final List<BlockId> blockIds = _insertBlockHeaders(blockHeaders, blockHeaderDatabaseManager);
            if ( (blockIds == null) || (blockIds.isEmpty()) ) {
                TransactionUtil.rollbackTransaction(databaseConnection);

                final BlockHeader firstBlockHeader = blockHeaders.get(0);
                Logger.info("Invalid BlockHeader: " + firstBlockHeader.getHash());

                return false;
            }

            final BlockId firstBlockHeaderId = blockIds.get(0);
            final Long firstBlockHeight = blockHeaderDatabaseManager.getBlockHeight(firstBlockHeaderId);

            final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(firstBlockHeaderId);

            final BlockHeaderValidatorContext blockHeaderValidatorContext = new BlockHeaderValidatorContext(blockchainSegmentId, databaseManager, _networkTime);
            final BlockHeaderValidator<?> blockHeaderValidator = new BlockHeaderValidator<>(blockHeaderValidatorContext);

            long nextBlockHeight = firstBlockHeight;
            for (final BlockHeader blockHeader : blockHeaders) {
                final BlockHeaderValidator.BlockHeaderValidationResult blockHeaderValidationResult = blockHeaderValidator.validateBlockHeader(blockHeader, nextBlockHeight);
                if (!blockHeaderValidationResult.isValid) {
                    Logger.info("Invalid BlockHeader: " + blockHeaderValidationResult.errorMessage);
                    TransactionUtil.rollbackTransaction(databaseConnection);
                    return false;
                }

                nextBlockHeight += 1L;
            }

            final long blockHeight = (nextBlockHeight - 1L);
            _headBlockHeight = Math.max(blockHeight, _headBlockHeight);

            TransactionUtil.commitTransaction(databaseConnection);

            return true;
        }
    }

    protected void _processBlockHeaders(final List<BlockHeader> blockHeaders) {
        final MilliTimer storeHeadersTimer = new MilliTimer();
        storeHeadersTimer.start();

        final BlockHeader firstBlockHeader = blockHeaders.get(0);
        Logger.debug("DOWNLOADED BLOCK HEADERS: "+ firstBlockHeader.getHash() + " + " + blockHeaders.getCount());

        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final Boolean headersAreValid = _validateAndStoreBlockHeaders(blockHeaders, databaseManager);
            if (! headersAreValid) { return; } // TODO: Prevent attempting to reprocess invalid Block Headers (e.g. DOS)...

            for (final BlockHeader blockHeader : blockHeaders) {
                final Sha256Hash blockHash = blockHeader.getHash();

                _threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (_blockDownloadRequester != null) {
                            _blockDownloadRequester.requestBlock(blockHeader);
                        }
                    }
                });

                _lastBlockHash = blockHash;
                _lastBlockHeader = blockHeader;
            }

            _blockHeaderCount += blockHeaders.getCount();
            _timer.stop();
            final Long millisecondsElapsed = _timer.getMillisecondsElapsed();
            _averageBlockHeadersPerSecond.value = ( (_blockHeaderCount.floatValue() / millisecondsElapsed) * 1000L );
        }
        catch (final DatabaseException exception) {
            Logger.warn("Processing BlockHeaders failed.", exception);
            return;
        }

        storeHeadersTimer.stop();
        Logger.info("Stored Block Headers: " + firstBlockHeader.getHash() + " - " + _lastBlockHash + " (" + storeHeadersTimer.getMillisecondsElapsed() + "ms)");
    }

    public BlockHeaderDownloader(final DatabaseManagerFactory databaseManagerFactory, final BitcoinNodeManager nodeManager, final NetworkTime networkTime, final BlockDownloadRequester blockDownloadRequester, final ThreadPool threadPool) {
        _databaseManagerFactory = databaseManagerFactory;
        _nodeManager = nodeManager;
        _networkTime = networkTime;
        _blockDownloadRequester = blockDownloadRequester;
        _timer = new MilliTimer();
        _threadPool = threadPool;

        _downloadBlockHeadersCallback = new BitcoinNodeManager.DownloadBlockHeadersCallback() {
            @Override
            public void onResult(final List<BlockHeader> blockHeaders) {
                if (! _isProcessingHeaders.compareAndSet(false, true)) { return; }

                try {
                    _processBlockHeaders(blockHeaders);

                    final Runnable newBlockHeaderAvailableCallback = _newBlockHeaderAvailableCallback;
                    if (newBlockHeaderAvailableCallback != null) {
                        _threadPool.execute(newBlockHeaderAvailableCallback);
                    }
                }
                finally {
                    _isProcessingHeaders.set(false);
                    synchronized (_isProcessingHeaders) {
                        _isProcessingHeaders.notifyAll();
                    }

                    synchronized (_headersDownloadedPin) {
                        _headersDownloadedPin.notifyAll();
                    }
                }
            }

            @Override
            public void onFailure() {
                // Let the headersDownloadedPin timeout...
            }
        };
    }

    @Override
    protected void _onStart() {
        _timer.start();
        _blockHeaderCount = 0L;

        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId headBlockId = blockHeaderDatabaseManager.getHeadBlockHeaderId();
            if (headBlockId != null) {
                _lastBlockHash = blockHeaderDatabaseManager.getBlockHash(headBlockId);
                _headBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);
            }
            else {
                _lastBlockHash = Block.GENESIS_BLOCK_HASH;
                _headBlockHeight = 0L;
            }
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            _lastBlockHash = Util.coalesce(_lastBlockHash, Block.GENESIS_BLOCK_HASH);
        }

        if (! _checkForGenesisBlockHeader()) {
            _downloadGenesisBlock();
        }
    }

    @Override
    protected Boolean _run() {
        synchronized (_genesisBlockPin) {
            while (! _hasGenesisBlock) {
                try { _genesisBlockPin.wait(); }
                catch (final InterruptedException exception) { return false; }
            }
        }

        _nodeManager.requestBlockHeadersAfter(_lastBlockHash, _downloadBlockHeadersCallback);

        synchronized (_headersDownloadedPin) {
            final MilliTimer timer = new MilliTimer();
            timer.start();

            final boolean didTimeout;

            try { _headersDownloadedPin.wait(MAX_TIMEOUT_MS); }
            catch (final InterruptedException exception) { return false; }

            // If the _headersDownloadedPin timed out because processing the headers took too long, wait for the processing to complete and then consider it a success.
            synchronized (_isProcessingHeaders) {
                if (_isProcessingHeaders.get()) {
                    try { _isProcessingHeaders.wait(); }
                    catch (final InterruptedException exception) { return false; }

                    didTimeout = false;
                }
                else {
                    timer.stop();
                    didTimeout = (timer.getMillisecondsElapsed() >= MAX_TIMEOUT_MS);
                }
            }

            if (didTimeout) {
                // The lastBlockHeader may be null when first starting.
                if (_lastBlockHeader == null) { return true; }

                // Don't sleep after a timeout while the most recent block timestamp is less than the minBlockTimestamp...
                return (_lastBlockHeader.getTimestamp() < _minBlockTimestamp);
            }
        }

        return true;
    }

    @Override
    protected void _onSleep() { }

    public void setNewBlockHeaderAvailableCallback(final Runnable newBlockHeaderAvailableCallback) {
        _newBlockHeaderAvailableCallback = newBlockHeaderAvailableCallback;
    }

    /**
     * Sets the minimum expected block timestamp (in seconds).
     *  The BlockHeaderDownloader will not go to sleep (unless interrupted) before its most recent blockHeader's
     *  timestamp is at least the minBlockTimestamp.
     */
    public void setMinBlockTimestamp(final Long minBlockTimestampInSeconds) {
        _minBlockTimestamp = minBlockTimestampInSeconds;
    }

    public Container<Float> getAverageBlockHeadersPerSecondContainer() {
        return _averageBlockHeadersPerSecond;
    }

    public Long getBlockHeight() {
        return _headBlockHeight;
    }

    /**
     * When headers are received, they are processed as a batch.
     *  After each batch completes, the NewBlockHeaderAvailableCallback is invoked.
     *  This setting controls the size of the batch.
     *  The default value is 2000.
     */
    public void setMaxHeaderBatchSize(final Integer batchSize) {
        _maxHeaderBatchSize = batchSize;
    }

    public Integer getMaxHeaderBatchSize() {
        return _maxHeaderBatchSize;
    }
}
