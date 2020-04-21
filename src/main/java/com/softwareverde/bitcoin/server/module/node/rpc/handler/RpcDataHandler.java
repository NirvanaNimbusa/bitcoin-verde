package com.softwareverde.bitcoin.server.module.node.rpc.handler;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.validator.BlockValidationResult;
import com.softwareverde.bitcoin.block.validator.BlockValidator;
import com.softwareverde.bitcoin.block.validator.ValidationResult;
import com.softwareverde.bitcoin.block.validator.difficulty.DifficultyCalculator;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.BlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.transaction.TransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.FullNodeTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.slp.SlpTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.rpc.NodeRpcHandler;
import com.softwareverde.bitcoin.server.module.node.store.BlockStore;
import com.softwareverde.bitcoin.server.module.node.sync.SlpTransactionProcessor;
import com.softwareverde.bitcoin.server.module.node.sync.block.BlockDownloader;
import com.softwareverde.bitcoin.server.module.node.sync.transaction.TransactionDownloader;
import com.softwareverde.bitcoin.slp.SlpTokenId;
import com.softwareverde.bitcoin.slp.validator.SlpTransactionValidationCache;
import com.softwareverde.bitcoin.slp.validator.SlpTransactionValidator;
import com.softwareverde.bitcoin.slp.validator.TransactionAccumulator;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionId;
import com.softwareverde.bitcoin.transaction.TransactionWithFee;
import com.softwareverde.bitcoin.transaction.validator.TransactionValidator;
import com.softwareverde.bitcoin.transaction.validator.TransactionValidatorFactory;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableListBuilder;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.util.TransactionUtil;
import com.softwareverde.logging.Logger;
import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.util.Container;

public class RpcDataHandler implements NodeRpcHandler.DataHandler {
    protected final FullNodeDatabaseManagerFactory _databaseManagerFactory;
    protected final TransactionDownloader _transactionDownloader;
    protected final BlockValidator _blockValidator;
    protected final TransactionValidatorFactory _transactionValidatorFactory;
    protected final BlockDownloader _blockDownloader;
    protected final BlockStore _blockStore;

    public RpcDataHandler(final FullNodeDatabaseManagerFactory databaseManagerFactory, final TransactionDownloader transactionDownloader, final BlockDownloader blockDownloader, final BlockValidator blockValidator, final TransactionValidatorFactory transactionValidatorFactory, final BlockStore blockStore) {
        _databaseManagerFactory = databaseManagerFactory;

        _transactionDownloader = transactionDownloader;
        _blockDownloader = blockDownloader;
        _blockValidator = blockValidator;
        _transactionValidatorFactory = transactionValidatorFactory;
        _blockStore = blockStore;
    }

    @Override
    public Long getBlockHeaderHeight() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId blockId = blockHeaderDatabaseManager.getHeadBlockHeaderId();
            if (blockId == null) { return 0L; }

            return blockHeaderDatabaseManager.getBlockHeight(blockId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Long getBlockHeight() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId blockId = blockDatabaseManager.getHeadBlockId();
            if (blockId == null) { return 0L; }

            return blockHeaderDatabaseManager.getBlockHeight(blockId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Long getBlockHeaderTimestamp() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId headBlockId = blockHeaderDatabaseManager.getHeadBlockHeaderId();
            if (headBlockId == null) { return MedianBlockTime.GENESIS_BLOCK_TIMESTAMP; }

            return blockHeaderDatabaseManager.getBlockTimestamp(headBlockId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Long getBlockTimestamp() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
            if (headBlockId == null) { return MedianBlockTime.GENESIS_BLOCK_TIMESTAMP; }

            return blockHeaderDatabaseManager.getBlockTimestamp(headBlockId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public List<BlockHeader> getBlockHeaders(final Long nullableBlockHeight, final Integer maxBlockCount) {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final Long startingBlockHeight;
            {
                if (nullableBlockHeight != null) {
                    startingBlockHeight = nullableBlockHeight;
                }
                else {
                    final BlockId headBlockId = blockHeaderDatabaseManager.getHeadBlockHeaderId();
                    startingBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);
                }
            }

            final BlockchainSegmentId blockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final ImmutableListBuilder<BlockHeader> blockHeaders = new ImmutableListBuilder<BlockHeader>(maxBlockCount);
            for (int i = 0; i < maxBlockCount; ++i) {
                if (startingBlockHeight < i) { break; }

                final BlockId blockId = blockHeaderDatabaseManager.getBlockIdAtHeight(blockchainSegmentId, (startingBlockHeight - i));
                if (blockId == null) { break; }

                final BlockHeader blockHeader = blockHeaderDatabaseManager.getBlockHeader(blockId);
                if (blockHeader == null) { continue; }

                blockHeaders.add(blockHeader);
            }
            return blockHeaders.build();
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public BlockHeader getBlockHeader(final Long blockHeight) {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();

            final BlockchainSegmentId headBlockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final BlockId blockId = blockHeaderDatabaseManager.getBlockIdAtHeight(headBlockchainSegmentId, blockHeight);
            if (blockId == null) { return null; }

            return blockHeaderDatabaseManager.getBlockHeader(blockId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public BlockHeader getBlockHeader(final Sha256Hash blockHash) {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(blockHash);
            if (blockId == null) { return null; }

            return blockHeaderDatabaseManager.getBlockHeader(blockId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Long getBlockHeaderHeight(final Sha256Hash blockHash) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(blockHash);
            final Long blockHeaderHeight = blockHeaderDatabaseManager.getBlockHeight(blockId);
            return blockHeaderHeight;
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Block getBlock(final Long blockHeight) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();

            final BlockchainSegmentId headBlockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final BlockId blockId = blockHeaderDatabaseManager.getBlockIdAtHeight(headBlockchainSegmentId, blockHeight);
            if (blockId == null) { return null; }

            if (_blockStore != null) {
                final Sha256Hash blockHash = blockHeaderDatabaseManager.getBlockHash(blockId);
                final Block cachedBlock = _blockStore.getBlock(blockHash, blockHeight);
                if (cachedBlock != null) {
                    return cachedBlock;
                }
            }

            final Block block = blockDatabaseManager.getBlock(blockId);

            if (_blockStore != null) {
                _blockStore.storeBlock(block, blockHeight);
            }

            return block;
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Block getBlock(final Sha256Hash blockHash) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();

            final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(blockHash);
            if (blockId == null) { return null; }

            final Long blockHeight = (_blockStore != null ? blockHeaderDatabaseManager.getBlockHeight(blockId) : null);

            if (_blockStore != null) {
                final Block cachedBlock = _blockStore.getBlock(blockHash, blockHeight);
                if (cachedBlock != null) {
                    return cachedBlock;
                }
            }

            final Block block = blockDatabaseManager.getBlock(blockId);
            if (block == null) { return null; }

            if (_blockStore != null) {
                _blockStore.storeBlock(block, blockHeight);
            }

            return block;
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Transaction getTransaction(final Sha256Hash transactionHash) {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final TransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();

            final TransactionId transactionId = transactionDatabaseManager.getTransactionId(transactionHash);
            if (transactionId == null) { return null; }

            return transactionDatabaseManager.getTransaction(transactionId);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Difficulty getDifficulty() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final DifficultyCalculator difficultyCalculator = new DifficultyCalculator(databaseManager);
            return difficultyCalculator.calculateRequiredDifficulty();
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public List<Transaction> getUnconfirmedTransactions() {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();

            final List<TransactionId> unconfirmedTransactionIds = transactionDatabaseManager.getUnconfirmedTransactionIds();

            final ImmutableListBuilder<Transaction> unconfirmedTransactionsListBuilder = new ImmutableListBuilder<Transaction>(unconfirmedTransactionIds.getCount());
            for (final TransactionId transactionId : unconfirmedTransactionIds) {
                final Transaction transaction = transactionDatabaseManager.getTransaction(transactionId);
                unconfirmedTransactionsListBuilder.add(transaction);
            }

            return unconfirmedTransactionsListBuilder.build();
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public List<TransactionWithFee> getUnconfirmedTransactionsWithFees() {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();

            final List<TransactionId> unconfirmedTransactionIds = transactionDatabaseManager.getUnconfirmedTransactionIds();

            final ImmutableListBuilder<TransactionWithFee> listBuilder = new ImmutableListBuilder<TransactionWithFee>(unconfirmedTransactionIds.getCount());
            for (final TransactionId transactionId : unconfirmedTransactionIds) {
                final Transaction transaction = transactionDatabaseManager.getTransaction(transactionId);
                if (transaction == null) {
                    Logger.warn("Unable to load Unconfirmed Transaction: " + transactionId);
                    continue;
                }
                final Long transactionFee = transactionDatabaseManager.calculateTransactionFee(transaction);

                final TransactionWithFee transactionWithFee = new TransactionWithFee(transaction, transactionFee);
                listBuilder.add(transactionWithFee);
            }

            return listBuilder.build();
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Long getBlockReward() {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();

            final BlockId blockId = blockDatabaseManager.getHeadBlockId();
            if (blockId == null) { return 0L; }

            final Long blockHeight = blockHeaderDatabaseManager.getBlockHeight(blockId);

            return BlockHeader.calculateBlockReward(blockHeight);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Boolean isSlpTransaction(final Sha256Hash transactionHash) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final SlpTokenId slpTokenId = transactionDatabaseManager.getSlpTokenId(transactionHash);
            return (slpTokenId != null);
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public Boolean isValidSlpTransaction(final Sha256Hash transactionHash) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final SlpTransactionDatabaseManager slpTransactionDatabaseManager = databaseManager.getSlpTransactionDatabaseManager();

            final TransactionId transactionId = transactionDatabaseManager.getTransactionId(transactionHash);
            if (transactionId == null) { return false; }

            final BlockchainSegmentId blockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            return slpTransactionDatabaseManager.getSlpTransactionValidationResult(blockchainSegmentId, transactionId);
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public SlpTokenId getSlpTokenId(final Sha256Hash transactionHash) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            return transactionDatabaseManager.getSlpTokenId(transactionHash);
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    @Override
    public BlockValidationResult validatePrototypeBlock(final Block block) {
        Logger.info("Validating Prototype Block: " + block.getHash());

        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();

            try {
                synchronized (BlockHeaderDatabaseManager.MUTEX) {
                    TransactionUtil.startTransaction(databaseConnection);

                    final BlockId blockId = blockDatabaseManager.storeBlock(block);
                    return _blockValidator.validatePrototypeBlock(blockId, block);
                }
            }
            finally {
                TransactionUtil.rollbackTransaction(databaseConnection); // Never keep the validated block...
            }
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return BlockValidationResult.invalid("An internal error occurred.");
        }
    }

    @Override
    public ValidationResult validateTransaction(final Transaction transaction, final Boolean enableSlpValidation) {
        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final TransactionValidator transactionValidator = _transactionValidatorFactory.newTransactionValidator(databaseManager, null, null); // TODO: both should not be null...

            final Container<BlockchainSegmentId> blockchainSegmentIdContainer = new Container<BlockchainSegmentId>();

            try {
                TransactionUtil.startTransaction(databaseConnection);
                blockchainSegmentIdContainer.value = blockchainDatabaseManager.getHeadBlockchainSegmentId();
                final BlockId headBlockId = blockHeaderDatabaseManager.getHeadBlockHeaderId();
                final Long blockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);

                transactionDatabaseManager.storeTransaction(transaction);
                final Boolean isValidTransaction =  transactionValidator.validateTransaction(blockchainSegmentIdContainer.value, blockHeight, transaction, true);
                if (! isValidTransaction) {
                    return ValidationResult.invalid("Invalid Transaction.");
                }

                if (enableSlpValidation) {
                    final TransactionAccumulator transactionAccumulator = SlpTransactionProcessor.createTransactionAccumulator(blockchainSegmentIdContainer, databaseManager, null);
                    final SlpTransactionValidationCache slpTransactionValidationCache = SlpTransactionProcessor.createSlpTransactionValidationCache(blockchainSegmentIdContainer, databaseManager);
                    final SlpTransactionValidator slpTransactionValidator = new SlpTransactionValidator(transactionAccumulator, slpTransactionValidationCache);

                    final Boolean isValidSlpTransaction = slpTransactionValidator.validateTransaction(transaction);
                    if (! isValidSlpTransaction) {
                        return ValidationResult.invalid("Invalid SLP Transaction.");
                    }
                }

                return ValidationResult.valid();
            }
            finally {
                TransactionUtil.rollbackTransaction(databaseConnection); // Never keep the validated transaction...
            }
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return ValidationResult.invalid("An internal error occurred.");
        }
    }

    @Override
    public void submitTransaction(final Transaction transaction) {
        _transactionDownloader.submitTransaction(transaction);
    }

    @Override
    public void submitBlock(final Block block) {
        _blockDownloader.submitBlock(block);
    }
}
