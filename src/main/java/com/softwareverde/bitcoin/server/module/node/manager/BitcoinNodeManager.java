package com.softwareverde.bitcoin.server.module.node.manager;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.MerkleBlock;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.block.thin.AssembleThinBlockResult;
import com.softwareverde.bitcoin.block.thin.ThinBlockAssembler;
import com.softwareverde.bitcoin.server.SynchronizationStatus;
import com.softwareverde.bitcoin.server.configuration.BitcoinProperties;
import com.softwareverde.bitcoin.server.message.type.node.address.BitcoinNodeIpAddress;
import com.softwareverde.bitcoin.server.message.type.node.feature.NodeFeatures;
import com.softwareverde.bitcoin.server.module.node.MemoryPoolEnquirer;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.block.BlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.node.BitcoinNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.manager.banfilter.BanFilter;
import com.softwareverde.bitcoin.server.module.node.sync.BlockFinderHashesBuilder;
import com.softwareverde.bitcoin.server.module.node.sync.inventory.BitcoinNodeHeadBlockFinder;
import com.softwareverde.bitcoin.server.node.BitcoinNode;
import com.softwareverde.bitcoin.server.node.BitcoinNodeFactory;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bloomfilter.BloomFilter;
import com.softwareverde.bloomfilter.MutableBloomFilter;
import com.softwareverde.concurrent.pool.ThreadPool;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.ip.Ip;
import com.softwareverde.network.p2p.node.address.NodeIpAddress;
import com.softwareverde.network.p2p.node.manager.NodeManager;
import com.softwareverde.network.time.MutableNetworkTime;
import com.softwareverde.util.Util;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class BitcoinNodeManager extends NodeManager<BitcoinNode> {
    public static final Integer MINIMUM_THIN_BLOCK_TRANSACTION_COUNT = 64;
    public interface FailableCallback {
        default void onFailure() { }
    }
    public interface BlockInventoryMessageCallback extends BitcoinNode.BlockInventoryMessageCallback, FailableCallback { }
    public interface DownloadBlockCallback extends BitcoinNode.DownloadBlockCallback {
        default void onFailure(Sha256Hash blockHash) { }
    }
    public interface DownloadMerkleBlockCallback extends BitcoinNode.DownloadMerkleBlockCallback {
        default void onFailure(Sha256Hash blockHash) { }
    }
    public interface DownloadBlockHeadersCallback extends FailableCallback {
        void onResult(List<BlockHeader> blockHeaders, BitcoinNode bitcoinNode);
    }
    public interface DownloadTransactionCallback extends BitcoinNode.DownloadTransactionCallback {
        default void onFailure(List<Sha256Hash> transactionHashes) { }
    }
    public interface NewNodeCallback {
        void onNodeHandshakeComplete(BitcoinNode bitcoinNode);
    }

    public static class Context {
        public Integer maxNodeCount;
        public DatabaseManagerFactory databaseManagerFactory;
        public BitcoinNodeFactory nodeFactory;
        public MutableNetworkTime networkTime;
        public NodeInitializer nodeInitializer;
        public BanFilter banFilter;
        public MemoryPoolEnquirer memoryPoolEnquirer;
        public SynchronizationStatus synchronizationStatusHandler;
        public ThreadPool threadPool;
    }

    protected final DatabaseManagerFactory _databaseManagerFactory;
    protected final NodeInitializer _nodeInitializer;
    protected final BanFilter _banFilter;
    protected final MemoryPoolEnquirer _memoryPoolEnquirer;
    protected final SynchronizationStatus _synchronizationStatusHandler;
    protected final BitcoinNodeHeadBlockFinder _bitcoinNodeHeadBlockFinder;
    protected final AtomicBoolean _hasHadActiveConnectionSinceLastDisconnect = new AtomicBoolean(false);
    protected final MutableList<String> _dnsSeeds = new MutableList<String>(0);

    protected Boolean _transactionRelayIsEnabled = true;
    protected Boolean _slpValidityCheckingIsEnabled = false;
    protected Boolean _newBlocksViaHeadersIsEnabled = true;
    protected MutableBloomFilter _bloomFilter = null;

    protected final Object _pollForReconnectionThreadMutex = new Object();
    protected final Runnable _pollForReconnection = new Runnable() {
        @Override
        public void run() {
            final long maxWait = (5L * 60L * 1000L); // 5 Minutes...
            long nextWait = 500L;
            while (! Thread.interrupted()) {
                if (_isShuttingDown) { return; }

                try { Thread.sleep(nextWait); }
                catch (final Exception exception) { break; }

                final MutableList<NodeIpAddress> nodeIpAddresses;
                if (_shouldOnlyConnectToSeedNodes) {
                    nodeIpAddresses = new MutableList<NodeIpAddress>(_seedNodes);
                }
                else {
                    final HashSet<String> seedNodeSet = new HashSet<String>();
                    nodeIpAddresses = new MutableList<NodeIpAddress>(0);

                    { // Add seed nodes...
                        for (final NodeIpAddress nodeIpAddress : _seedNodes) {
                            final Ip ip = nodeIpAddress.getIp();
                            final String ipString = ip.toString();
                            final Integer port = nodeIpAddress.getPort();
                            seedNodeSet.add(ipString + port);

                            nodeIpAddresses.add(nodeIpAddress);
                        }
                    }

                    { // Add previously-connected nodes...
                        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
                            final BitcoinNodeDatabaseManager nodeDatabaseManager = databaseManager.getNodeDatabaseManager();

                            final MutableList<NodeFeatures.Feature> requiredFeatures = new MutableList<NodeFeatures.Feature>();
                            requiredFeatures.add(NodeFeatures.Feature.BLOCKCHAIN_ENABLED);
                            requiredFeatures.add(NodeFeatures.Feature.BITCOIN_CASH_ENABLED);
                            for (final BitcoinNodeIpAddress nodeIpAddress : nodeDatabaseManager.findNodes(requiredFeatures, _maxNodeCount)) {
                                if (nodeIpAddresses.getCount() >= _maxNodeCount) { break; }
                                nodeIpAddresses.add(nodeIpAddress);
                            }
                        }
                        catch (final DatabaseException databaseException) {
                            Logger.warn(databaseException);
                        }
                    }

                    { // Connect to DNS seeded nodes...
                        final Integer defaultPort = BitcoinProperties.PORT;
                        for (final String seedHost : _dnsSeeds) {
                            final List<Ip> seedIps = Ip.allFromHostName(seedHost);
                            if (seedIps == null) { continue; }

                            for (final Ip ip : seedIps) {
                                if (nodeIpAddresses.getCount() >= _maxNodeCount) { break; }

                                final String host = ip.toString();
                                if (seedNodeSet.contains(host + defaultPort)) { continue; } // Exclude SeedNodes...

                                nodeIpAddresses.add(new NodeIpAddress(ip, defaultPort));
                            }
                        }
                    }
                }

                for (final NodeIpAddress nodeIpAddress : nodeIpAddresses) {
                    final Ip ip = nodeIpAddress.getIp();
                    if (ip == null) { continue; }

                    final String host = ip.toString();
                    final Integer port = nodeIpAddress.getPort();
                    final BitcoinNode bitcoinNode = _nodeFactory.newNode(host, port);

                    _addNode(bitcoinNode); // NOTE: _addNotHandshakedNode(BitcoinNode) is not the same as addNode(BitcoinNode)...

                    Logger.info("All nodes disconnected.  Falling back on previously-seen node: " + host + ":" + ip);
                }

                nextWait = Math.min((2L * nextWait), maxWait);
            }
            _pollForReconnectionThread = null;
        }
    };
    protected Thread _pollForReconnectionThread;

    protected Runnable _onNodeListChanged;
    protected NewNodeCallback _onNewNode;

    // BitcoinNodeManager::transmitBlockHash is often called in rapid succession with the same BlockHash, therefore a simple cache is used...
    protected BlockHeaderWithTransactionCount _cachedTransmittedBlockHeader = null;

    protected void _pollForReconnection() {
        if (_isShuttingDown) { return; }

        synchronized (_pollForReconnectionThreadMutex) {
            if (_pollForReconnectionThread != null) { return; }

            _pollForReconnectionThread = new Thread(_pollForReconnection);
            _pollForReconnectionThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(final Thread thread, final Throwable exception) {
                    Logger.error("Uncaught exception in thread.", exception);
                }
            });
            _pollForReconnectionThread.start();
        }
    }

    @Override
    protected void _initNode(final BitcoinNode node) {
        node.enableTransactionRelay(_transactionRelayIsEnabled);

        super._initNode(node);
        _nodeInitializer.initializeNode(node);
    }

    @Override
    protected void _onAllNodesDisconnected() {
        if (! _hasHadActiveConnectionSinceLastDisconnect.getAndSet(false)) { return; } // Prevent infinitely looping by aborting if no new connections were successful since the last attempt...
        _pollForReconnection();
    }

    @Override
    protected void _onNodeConnected(final BitcoinNode bitcoinNode) {
        _hasHadActiveConnectionSinceLastDisconnect.set(true); // Allow for reconnection attempts after all connections die...

        { // Abort the reconnection Thread, if it is running...
            final Thread pollForReconnectionThread = _pollForReconnectionThread;
            if (pollForReconnectionThread != null) {
                pollForReconnectionThread.interrupt();
            }
        }

        bitcoinNode.ping(null);

        final BloomFilter bloomFilter = _bloomFilter;
        if (bloomFilter != null) {
            bitcoinNode.setBloomFilter(bloomFilter);
        }

        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BlockFinderHashesBuilder blockFinderHashesBuilder = new BlockFinderHashesBuilder(databaseManager);
            final List<Sha256Hash> blockFinderHashes = blockFinderHashesBuilder.createBlockFinderBlockHashes();

            bitcoinNode.transmitBlockFinder(blockFinderHashes);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
        }

        final Runnable onNodeListChangedCallback = _onNodeListChanged;
        if (onNodeListChangedCallback != null) {
            _threadPool.execute(onNodeListChangedCallback);
        }
    }

    @Override
    protected void _onNodeDisconnected(final BitcoinNode bitcoinNode) {
        super._onNodeDisconnected(bitcoinNode);

        final Ip ip = bitcoinNode.getIp();
        _banFilter.onNodeDisconnected(ip);

        final Runnable onNodeListChangedCallback = _onNodeListChanged;
        if (onNodeListChangedCallback != null) {
            _threadPool.execute(onNodeListChangedCallback);
        }
    }

    @Override
    protected void _addHandshakedNode(final BitcoinNode node) {
        if (_isShuttingDown) {
            node.disconnect();
            return;
        }

        final Boolean blockchainIsEnabled = node.hasFeatureEnabled(NodeFeatures.Feature.BLOCKCHAIN_ENABLED);
        final Boolean blockchainIsSynchronized = _synchronizationStatusHandler.isBlockchainSynchronized();
        if (blockchainIsEnabled == null) {
            Logger.debug("Unable to determine feature for node: " + node.getConnectionString());
        }

        if ( (! Util.coalesce(blockchainIsEnabled, false)) && (! blockchainIsSynchronized) ) {
            node.disconnect();
            return; // Reject SPV Nodes during the initial-sync...
        }

        super._addHandshakedNode(node);
    }

    @Override
    protected void _addNotHandshakedNode(final BitcoinNode bitcoinNode) {
        final NodeIpAddress nodeIpAddress = bitcoinNode.getRemoteNodeIpAddress();

        final Ip ip = nodeIpAddress.getIp();
        final Boolean isBanned = _banFilter.isIpBanned(ip);
        if ( (_isShuttingDown) || (isBanned) ) {
            _removeNode(bitcoinNode);
            return;
        }

        super._addNotHandshakedNode(bitcoinNode);

        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BitcoinNodeDatabaseManager nodeDatabaseManager = databaseManager.getNodeDatabaseManager();
            nodeDatabaseManager.storeNode(bitcoinNode);
        }
        catch (final DatabaseException databaseException) {
            Logger.warn(databaseException);
        }
    }

    @Override
    protected void _onNodeHandshakeComplete(final BitcoinNode bitcoinNode) {
        if (_slpValidityCheckingIsEnabled) {
            if (Util.coalesce(bitcoinNode.hasFeatureEnabled(NodeFeatures.Feature.SLP_INDEX_ENABLED), false)) {
                bitcoinNode.enableSlpValidityChecking(true);
            }
        }

        if (_newBlocksViaHeadersIsEnabled) {
            bitcoinNode.enableNewBlockViaHeaders();
        }

        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final BitcoinNodeDatabaseManager nodeDatabaseManager = databaseManager.getNodeDatabaseManager();

            nodeDatabaseManager.updateLastHandshake(bitcoinNode); // WARNING: If removing Last Handshake update, ensure BanFilter no longer requires the handshake timestamp...
            nodeDatabaseManager.updateNodeFeatures(bitcoinNode);
            nodeDatabaseManager.updateUserAgent(bitcoinNode);
        }
        catch (final DatabaseException databaseException) {
            Logger.debug(databaseException);
        }

        final NewNodeCallback newNodeCallback = _onNewNode;
        if (newNodeCallback != null) {
            _threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    newNodeCallback.onNodeHandshakeComplete(bitcoinNode);
                }
            });
        }

        _banFilter.onNodeHandshakeComplete(bitcoinNode);

        final Runnable onNodeListChangedCallback = _onNodeListChanged;
        if (onNodeListChangedCallback != null) {
            _threadPool.execute(onNodeListChangedCallback);
        }
    }

    public BitcoinNodeManager(final Context context) {
        super(context.maxNodeCount, context.nodeFactory, context.networkTime, context.threadPool);
        _databaseManagerFactory = context.databaseManagerFactory;
        _nodeInitializer = context.nodeInitializer;
        _banFilter = context.banFilter;
        _memoryPoolEnquirer = context.memoryPoolEnquirer;
        _synchronizationStatusHandler = context.synchronizationStatusHandler;

        _bitcoinNodeHeadBlockFinder = new BitcoinNodeHeadBlockFinder(_databaseManagerFactory, _threadPool, _banFilter);
    }

    protected void _requestBlockHeaders(final List<Sha256Hash> blockHashes, final DownloadBlockHeadersCallback callback) {
        _selectNodeForRequest(new NodeApiRequest<BitcoinNode>() {
            @Override
            public void run(final BitcoinNode bitcoinNode) {
                final NodeApiRequest<BitcoinNode> apiRequest = this;

                bitcoinNode.requestBlockHeaders(blockHashes, new BitcoinNode.DownloadBlockHeadersCallback() {
                    @Override
                    public void onResult(final List<BlockHeader> blockHeaders) {
                        _onResponseReceived(bitcoinNode, apiRequest);
                        if (apiRequest.didTimeout) { return; }

                        final Boolean shouldAcceptHeaders = _banFilter.onHeadersReceived(bitcoinNode, blockHeaders);
                        if (! shouldAcceptHeaders) {
                            Logger.info("Received invalid headers from " + bitcoinNode + ".");
                            callback.onFailure();
                            bitcoinNode.disconnect();
                            return;
                        }

                        if (callback != null) {
                            callback.onResult(blockHeaders, bitcoinNode);
                        }
                    }
                });
            }

            @Override
            public void onFailure() {
                final Sha256Hash firstBlockHash = (blockHashes.isEmpty() ? null : blockHashes.get(0));
                Logger.debug("Request failed: BitcoinNodeManager.requestBlockHeader("+ firstBlockHash +")");

                if (callback != null) {
                    callback.onFailure();
                }
            }
        });
    }

    public void broadcastBlockFinder(final List<Sha256Hash> blockHashes) {
        for (final BitcoinNode bitcoinNode : _nodes.values()) {
            bitcoinNode.transmitBlockFinder(blockHashes);
        }
    }

    public void requestBlockHashesAfter(final Sha256Hash blockHash) {
        _sendMessage(new NodeApiMessage<BitcoinNode>() {
            @Override
            public void run(final BitcoinNode bitcoinNode) {
                bitcoinNode.requestBlockHashesAfter(blockHash);
            }
        });
    }

    protected NodeApiRequest<BitcoinNode> _createRequestBlockRequest(final Sha256Hash blockHash, final DownloadBlockCallback callback) {
        return new NodeApiRequest<BitcoinNode>() {
            protected BitcoinNode _bitcoinNode;

            @Override
            public void run(final BitcoinNode bitcoinNode) {
                _bitcoinNode = bitcoinNode;

                final NodeApiRequest<BitcoinNode> apiRequest = this;

                bitcoinNode.requestBlock(blockHash, new BitcoinNode.DownloadBlockCallback() {
                    @Override
                    public void onResult(final Block block) {
                        _onResponseReceived(bitcoinNode, apiRequest);
                        if (apiRequest.didTimeout) { return; }

                        if (callback != null) {
                            Logger.debug("Received Block: "+ block.getHash() +" from Node: " + bitcoinNode.getConnectionString());
                            callback.onResult(block);
                        }
                    }

                    @Override
                    public void onFailure(final Sha256Hash blockHash) {
                        if (apiRequest.didTimeout) { return; }

                        _pendingRequestsManager.removePendingRequest(apiRequest);

                        apiRequest.onFailure();
                    }
                });
            }

            @Override
            public void onFailure() {
                Logger.debug("Request failed: BitcoinNodeManager.requestBlock("+ blockHash +") " + (_bitcoinNode != null ? _bitcoinNode.getConnectionString() : "null"));

                if (callback != null) {
                    callback.onFailure(blockHash);
                }
            }
        };
    }

    protected void _requestBlock(final Sha256Hash blockHash, final DownloadBlockCallback callback) {
        _selectNodeForRequest(_createRequestBlockRequest(blockHash, callback));
    }

    protected void _requestBlock(final BitcoinNode selectedNode, final Sha256Hash blockHash, final DownloadBlockCallback callback) {
        _selectNodeForRequest(selectedNode, _createRequestBlockRequest(blockHash, callback));
    }

    protected void _requestMerkleBlock(final Sha256Hash blockHash, final DownloadMerkleBlockCallback callback) {
        final NodeApiRequest<BitcoinNode> downloadMerkleBlockRequest = new NodeApiRequest<BitcoinNode>() {
            protected BitcoinNode _bitcoinNode;

            @Override
            public void run(final BitcoinNode bitcoinNode) {
                _bitcoinNode = bitcoinNode;

                final NodeApiRequest<BitcoinNode> apiRequest = this;

                bitcoinNode.requestMerkleBlock(blockHash, new BitcoinNode.DownloadMerkleBlockCallback() {
                    @Override
                    public void onResult(final BitcoinNode.MerkleBlockParameters merkleBlockParameters) {
                        _onResponseReceived(bitcoinNode, apiRequest);
                        if (apiRequest.didTimeout) { return; }

                        final MerkleBlock merkleBlock = merkleBlockParameters.getMerkleBlock();
                        if (callback != null) {
                            Logger.debug("Received Merkle Block: "+ merkleBlock.getHash() +" from Node: " + bitcoinNode.getConnectionString());
                            callback.onResult(merkleBlockParameters);
                        }
                    }

                    @Override
                    public void onFailure(final Sha256Hash blockHash) {
                        if (apiRequest.didTimeout) { return; }

                        _pendingRequestsManager.removePendingRequest(apiRequest);

                        apiRequest.onFailure();
                    }
                });
            }

            @Override
            public void onFailure() {
                Logger.debug("Request failed: BitcoinNodeManager.requestMerkleBlock("+ blockHash +") " + (_bitcoinNode != null ? _bitcoinNode.getConnectionString() : "null"));

                if (callback != null) {
                    callback.onFailure(blockHash);
                }
            }
        };

        _selectNodeForRequest(downloadMerkleBlockRequest);
    }

    protected NodeApiRequest<BitcoinNode> _createRequestTransactionsRequest(final List<Sha256Hash> transactionHashes, final DownloadTransactionCallback callback) {
        return new NodeApiRequest<BitcoinNode>() {
            @Override
            public void run(final BitcoinNode bitcoinNode) {
                final NodeApiRequest<BitcoinNode> apiRequest = this;

                bitcoinNode.requestTransactions(transactionHashes, new BitcoinNode.DownloadTransactionCallback() {
                    @Override
                    public void onResult(final Transaction result) {
                        _onResponseReceived(bitcoinNode, apiRequest);
                        if (apiRequest.didTimeout) { return; }

                        if (callback != null) {
                            callback.onResult(result);
                        }
                    }

                    @Override
                    public void onFailure(final Sha256Hash transactionHash) {
                        if (apiRequest.didTimeout) { return; }

                        _pendingRequestsManager.removePendingRequest(apiRequest);

                        apiRequest.onFailure();
                    }
                });
            }

            @Override
            public void onFailure() {
                Logger.debug("Request failed: BitcoinNodeManager.requestTransactions("+ transactionHashes.get(0) +" + "+ (transactionHashes.getCount() - 1) +")");

                if (callback != null) {
                    callback.onFailure(transactionHashes);
                }
            }
        };
    }

    public void requestThinBlock(final Sha256Hash blockHash, final DownloadBlockCallback callback) {
        final NodeApiRequest<BitcoinNode> thinBlockApiRequest = new NodeApiRequest<BitcoinNode>() {
            @Override
            public void run(final BitcoinNode bitcoinNode) {
                final NodeApiRequest<BitcoinNode> apiRequest = this;

                final BloomFilter bloomFilter = _memoryPoolEnquirer.getBloomFilter(blockHash);

                bitcoinNode.requestThinBlock(blockHash, bloomFilter, new BitcoinNode.DownloadThinBlockCallback() { // TODO: Consider using ExtraThinBlocks... Unsure if the potential round-trip on a TransactionHash collision is worth it, though.
                    @Override
                    public void onResult(final BitcoinNode.ThinBlockParameters extraThinBlockParameters) {
                        _onResponseReceived(bitcoinNode, apiRequest);
                        if (apiRequest.didTimeout) { return; }

                        final BlockHeader blockHeader = extraThinBlockParameters.blockHeader;
                        final List<Sha256Hash> transactionHashes = extraThinBlockParameters.transactionHashes;
                        final List<Transaction> transactions = extraThinBlockParameters.transactions;

                        final ThinBlockAssembler thinBlockAssembler = new ThinBlockAssembler(_memoryPoolEnquirer);

                        final AssembleThinBlockResult assembleThinBlockResult = thinBlockAssembler.assembleThinBlock(blockHeader, transactionHashes, transactions);
                        if (! assembleThinBlockResult.wasSuccessful()) {
                            _selectNodeForRequest(bitcoinNode, new NodeApiRequest<BitcoinNode>() {
                                @Override
                                public void run(final BitcoinNode bitcoinNode) {
                                    final NodeApiRequest<BitcoinNode> apiRequest = this;

                                    bitcoinNode.requestThinTransactions(blockHash, assembleThinBlockResult.missingTransactions, new BitcoinNode.DownloadThinTransactionsCallback() {
                                        @Override
                                        public void onResult(final List<Transaction> missingTransactions) {
                                            _onResponseReceived(bitcoinNode, apiRequest);
                                            if (apiRequest.didTimeout) { return; }

                                            final Block block = thinBlockAssembler.reassembleThinBlock(assembleThinBlockResult, missingTransactions);
                                            if (block == null) {
                                                Logger.debug("NOTICE: Falling back to traditional block.");
                                                // Fallback on downloading block traditionally...
                                                _requestBlock(blockHash, callback);
                                            }
                                            else {
                                                Logger.debug("NOTICE: Thin block assembled. " + System.currentTimeMillis());
                                                if (callback != null) {
                                                    callback.onResult(assembleThinBlockResult.block);
                                                }
                                            }
                                        }
                                    });
                                }

                                @Override
                                public void onFailure() {
                                    Logger.debug("NOTICE: Falling back to traditional block.");

                                    _pendingRequestsManager.removePendingRequest(apiRequest);

                                    _requestBlock(blockHash, callback);
                                }
                            });
                        }
                        else {
                            Logger.debug("NOTICE: Thin block assembled on first trip. " + System.currentTimeMillis());
                            if (callback != null) {
                                callback.onResult(assembleThinBlockResult.block);
                            }
                        }
                    }
                });
            }

            @Override
            public void onFailure() {
                Logger.debug("Request failed: BitcoinNodeManager.requestThinBlock("+ blockHash +")");

                if (callback != null) {
                    callback.onFailure(blockHash);
                }
            }
        };

        final Boolean shouldRequestThinBlocks;
        {
            if (! _synchronizationStatusHandler.isBlockchainSynchronized()) {
                shouldRequestThinBlocks = false;
            }
            else if (_memoryPoolEnquirer == null) {
                shouldRequestThinBlocks = false;
            }
            else {
                final Integer memoryPoolTransactionCount = _memoryPoolEnquirer.getMemoryPoolTransactionCount();
                final Boolean memoryPoolIsTooEmpty = (memoryPoolTransactionCount >= MINIMUM_THIN_BLOCK_TRANSACTION_COUNT);
                shouldRequestThinBlocks = (! memoryPoolIsTooEmpty);
            }
        }

        if (shouldRequestThinBlocks) {
            final NodeFilter<BitcoinNode> nodeFilter = new NodeFilter<BitcoinNode>() {
                @Override
                public Boolean meetsCriteria(final BitcoinNode bitcoinNode) {
                    return bitcoinNode.supportsExtraThinBlocks();
                }
            };

            final BitcoinNode selectedNode = _selectBestNode(nodeFilter);
            if (selectedNode != null) {
                Logger.debug("NOTICE: Requesting thin block. " + System.currentTimeMillis());
                _selectNodeForRequest(selectedNode, thinBlockApiRequest);
            }
            else {
                _requestBlock(blockHash, callback);
            }
        }
        else {
            _requestBlock(blockHash, callback);
        }
    }

    public void requestBlock(final Sha256Hash blockHash, final DownloadBlockCallback callback) {
        _requestBlock(blockHash, callback);
    }

    public void requestBlock(final BitcoinNode selectedNode, final Sha256Hash blockHash, final DownloadBlockCallback callback) {
        if (selectedNode == null) {
            _requestBlock(blockHash, callback);
        }
        else {
            _requestBlock(selectedNode, blockHash, callback);
        }
    }

    public void requestMerkleBlock(final Sha256Hash blockHash, final DownloadMerkleBlockCallback callback) {
        if (_bloomFilter == null) {
            Logger.warn("Requesting MerkleBlock without a BloomFilter.");
        }

        _requestMerkleBlock(blockHash, callback);
    }

    public void broadcastTransactionHash(final Sha256Hash transactionHash) {
        final MutableList<Sha256Hash> transactionHashes = new MutableList<Sha256Hash>(1);
        transactionHashes.add(transactionHash);

        for (final BitcoinNode bitcoinNode : _nodes.values()) {
            if (! bitcoinNode.isTransactionRelayEnabled()) { continue; }

            bitcoinNode.transmitTransactionHashes(transactionHashes);
        }
    }

    public void transmitBlockHash(final BitcoinNode bitcoinNode, final Block block) {
        if (bitcoinNode.isNewBlocksViaHeadersEnabled()) {
            bitcoinNode.transmitBlockHeader(block, block.getTransactionCount());
        }
        else {
            final MutableList<Sha256Hash> blockHashes = new MutableList<Sha256Hash>(1);
            blockHashes.add(block.getHash());
            bitcoinNode.transmitBlockHashes(blockHashes);
        }
    }

    public void transmitBlockHash(final BitcoinNode bitcoinNode, final Sha256Hash blockHash) {
        if (bitcoinNode.isNewBlocksViaHeadersEnabled()) {
            final BlockHeaderWithTransactionCount cachedBlockHeader = _cachedTransmittedBlockHeader;
            if ( (cachedBlockHeader != null) && (Util.areEqual(blockHash, cachedBlockHeader.getHash())) ) {
                bitcoinNode.transmitBlockHeader(cachedBlockHeader);
            }
            else {
                try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
                    final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
                    final BlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();

                    final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(blockHash);
                    if (blockId == null) { return; } // Block Hash has not been synchronized...

                    final BlockHeader blockHeader = blockHeaderDatabaseManager.getBlockHeader(blockId);
                    final Integer transactionCount = blockDatabaseManager.getTransactionCount(blockId);
                    if (transactionCount == null) { return; } // Block Hash is currently only a header...

                    final BlockHeaderWithTransactionCount blockHeaderWithTransactionCount = new ImmutableBlockHeaderWithTransactionCount(blockHeader, transactionCount);
                    _cachedTransmittedBlockHeader = blockHeaderWithTransactionCount;

                    bitcoinNode.transmitBlockHeader(blockHeaderWithTransactionCount);
                }
                catch (final DatabaseException exception) {
                    Logger.warn(exception);
                }
            }
        }
        else {
            final MutableList<Sha256Hash> blockHashes = new MutableList<Sha256Hash>(1);
            blockHashes.add(blockHash);

            bitcoinNode.transmitBlockHashes(blockHashes);
        }
    }

    public void requestBlockHeadersAfter(final Sha256Hash blockHash, final DownloadBlockHeadersCallback callback) {
        final MutableList<Sha256Hash> blockHashes = new MutableList<Sha256Hash>(1);
        blockHashes.add(blockHash);

        _requestBlockHeaders(blockHashes, callback);
    }

    public void requestBlockHeadersAfter(final List<Sha256Hash> blockHashes, final DownloadBlockHeadersCallback callback) {
        _requestBlockHeaders(blockHashes, callback);
    }

    public void requestTransactions(final List<Sha256Hash> transactionHashes, final DownloadTransactionCallback callback) {
        if (transactionHashes.isEmpty()) { return; }

        _selectNodeForRequest(_createRequestTransactionsRequest(transactionHashes, callback));
    }

    public void requestTransactions(final BitcoinNode selectedNode, final List<Sha256Hash> transactionHashes, final DownloadTransactionCallback callback) {
        if (transactionHashes.isEmpty()) { return; }

        _selectNodeForRequest(selectedNode, _createRequestTransactionsRequest(transactionHashes, callback));
    }

    public Boolean hasBloomFilter() {
        return (_bloomFilter != null);
    }

    public void setBloomFilter(final MutableBloomFilter bloomFilter) {
        _bloomFilter = bloomFilter;

        for (final BitcoinNode bitcoinNode : _nodes.values()) {
            bitcoinNode.setBloomFilter(_bloomFilter);
        }
    }

    public void banNode(final Ip ip) {
        _banFilter.banIp(ip);

        // Disconnect all currently-connected nodes at that ip...
        final MutableList<BitcoinNode> droppedNodes = new MutableList<BitcoinNode>();

        for (final BitcoinNode bitcoinNode : _nodes.values()) {
            if (Util.areEqual(ip, bitcoinNode.getIp())) {
                droppedNodes.add(bitcoinNode);
            }
        }

        // Disconnect all pending nodes at that ip...
        for (final BitcoinNode bitcoinNode : _pendingNodes.values()) {
            if (Util.areEqual(ip, bitcoinNode.getIp())) {
                droppedNodes.add(bitcoinNode);
            }
        }

        for (final BitcoinNode bitcoinNode : droppedNodes) {
            _removeNode(bitcoinNode);
        }

        final Runnable onNodeListChangedCallback = _onNodeListChanged;
        if (onNodeListChangedCallback != null) {
            _threadPool.execute(onNodeListChangedCallback);
        }
    }

    public void unbanNode(final Ip ip) {
        _banFilter.unbanIp(ip);
    }

    public void addIpToWhitelist(final Ip ip) {
        _banFilter.addIpToWhitelist(ip);
    }

    public void removeIpFromWhitelist(final Ip ip) {
        _banFilter.removeIpFromWhitelist(ip);
    }

    public void setNodeListChangedCallback(final Runnable callback) {
        _onNodeListChanged = callback;
    }

    public void setNewNodeHandshakedCallback(final NewNodeCallback newNodeCallback) {
        _onNewNode = newNodeCallback;
    }

    public void enableTransactionRelay(final Boolean transactionRelayIsEnabled) {
        _transactionRelayIsEnabled = transactionRelayIsEnabled;

        for (final BitcoinNode bitcoinNode : _nodes.values()) {
            bitcoinNode.enableTransactionRelay(transactionRelayIsEnabled);
        }
    }

    public Boolean isTransactionRelayEnabled() {
        return _transactionRelayIsEnabled;
    }

    public void enableSlpValidityChecking(final Boolean shouldEnableSlpValidityChecking) {
        _slpValidityCheckingIsEnabled = shouldEnableSlpValidityChecking;
    }

    public Boolean isSlpValidityCheckingEnabled() {
        return _slpValidityCheckingIsEnabled;
    }

    public void enableNewBlockViaHeaders(final Boolean newBlocksViaHeadersIsEnabled) {
        _newBlocksViaHeadersIsEnabled = newBlocksViaHeadersIsEnabled;
        if (newBlocksViaHeadersIsEnabled) {
            for (final BitcoinNode bitcoinNode : _nodes.values()) {
                bitcoinNode.enableNewBlockViaHeaders();
            }
        }
    }

    public void defineDnsSeeds(final List<String> dnsSeeds) {
        _dnsSeeds.addAll(dnsSeeds);
    }

    @Override
    public void shutdown() {
        super.shutdown();

        final Thread pollForReconnectionThread = _pollForReconnectionThread;
        if (pollForReconnectionThread != null) {
            pollForReconnectionThread.interrupt();

            try { pollForReconnectionThread.join(5000L); } catch (final Exception exception) { }
        }
    }
}
