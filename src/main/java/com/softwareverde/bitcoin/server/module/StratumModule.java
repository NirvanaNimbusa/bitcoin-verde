package com.softwareverde.bitcoin.server.module;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderDeflater;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.hash.sha256.Sha256Hash;
import com.softwareverde.bitcoin.secp256k1.key.PrivateKey;
import com.softwareverde.bitcoin.server.Configuration;
import com.softwareverde.bitcoin.server.Constants;
import com.softwareverde.bitcoin.server.module.node.rpc.NodeJsonRpcConnection;
import com.softwareverde.bitcoin.server.stratum.message.RequestMessage;
import com.softwareverde.bitcoin.server.stratum.message.ResponseMessage;
import com.softwareverde.bitcoin.server.stratum.message.server.MinerSubmitBlockResult;
import com.softwareverde.bitcoin.server.stratum.socket.StratumServerSocket;
import com.softwareverde.bitcoin.server.stratum.task.StratumMineBlockTask;
import com.softwareverde.bitcoin.server.stratum.task.StratumMineBlockTaskFactory;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.concurrent.pool.MainThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableListBuilder;
import com.softwareverde.io.Logger;
import com.softwareverde.json.Json;
import com.softwareverde.network.socket.JsonProtocolMessage;
import com.softwareverde.network.socket.JsonSocket;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.HexUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MilliTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StratumModule {
    public static void execute(final String configurationFileName) {
        final StratumModule stratumModule = new StratumModule(configurationFileName);
        stratumModule.loop();
    }

    protected static final boolean PROXY_VIABTC = false;

    protected final Configuration _configuration;
    protected final StratumServerSocket _stratumServerSocket;
    protected final MainThreadPool _threadPool = new MainThreadPool(256, 60000L);

    protected final PrivateKey _privateKey;

    protected final Integer _extraNonceByteCount = 4;
    protected final Integer _extraNonce2ByteCount = 4;
    protected final Integer _totalExtraNonceByteCount = (_extraNonceByteCount + _extraNonce2ByteCount);
    protected final ByteArray _extraNonce;

    protected final SystemTime _systemTime = new SystemTime();

    protected final ReentrantReadWriteLock.WriteLock _mineBlockTaskWriteLock;
    protected final ReentrantReadWriteLock.ReadLock _mineBlockTaskReadLock;
    protected StratumMineBlockTaskFactory _stratumMineBlockTaskFactory;
    protected StratumMineBlockTask _currentMineBlockTask = null;
    protected final ConcurrentHashMap<Long, StratumMineBlockTask> _mineBlockTasks = new ConcurrentHashMap<Long, StratumMineBlockTask>();

    protected MilliTimer _lastTransactionQueueProcessTimer = new MilliTimer();
    protected final ConcurrentLinkedQueue<Transaction> _queuedTransactions = new ConcurrentLinkedQueue<Transaction>();

    protected Integer _shareDifficulty = 2048;

    protected final ConcurrentLinkedQueue<JsonSocket> _connections = new ConcurrentLinkedQueue<JsonSocket>();

    protected void _printError(final String errorMessage) {
        System.err.println(errorMessage);
    }

    protected void _broadcastNewTask(final Boolean abandonOldJobs) {
        final Iterator<JsonSocket> iterator = _connections.iterator();
        while (iterator.hasNext()) {
            final JsonSocket jsonSocket = iterator.next();
            if (jsonSocket == null) { continue; }

            _sendWork(jsonSocket, abandonOldJobs);
        }
    }

    protected Configuration _loadConfigurationFile(final String configurationFilename) {
        final File configurationFile =  new File(configurationFilename);
        if (! configurationFile.isFile()) {
            _printError("Invalid configuration file.");
            BitcoinUtil.exitFailure();
        }

        return new Configuration(configurationFile);
    }

    protected ByteArray _createRandomBytes(final int byteCount) {
        int i = 0;
        final MutableByteArray mutableByteArray = new MutableByteArray(byteCount);
        while (i < byteCount) {
            final byte[] randomBytes = ByteUtil.integerToBytes((int) (Math.random() * Integer.MAX_VALUE));
            for (byte b : randomBytes) {
                mutableByteArray.set(i, b);
                i += 1;
                if (i >= byteCount) { break; }
            }
        }
        return mutableByteArray;
    }

    protected NodeJsonRpcConnection _getNodeJsonRpcConnection() {
        final Configuration.ExplorerProperties explorerProperties = _configuration.getExplorerProperties();
        final String bitcoinRpcUrl = explorerProperties.getBitcoinRpcUrl();
        final Integer bitcoinRpcPort = explorerProperties.getBitcoinRpcPort();

        try {
            final Socket socket = new Socket(bitcoinRpcUrl, bitcoinRpcPort);
            if (socket.isConnected()) {
                return new NodeJsonRpcConnection(socket, _threadPool);
            }
        }
        catch (final Exception exception) {
            Logger.log(exception);
        }

        return null;
    }

    protected void _rebuildNewMiningTask() {
        final StratumMineBlockTaskFactory stratumMineBlockTaskFactory = new StratumMineBlockTaskFactory();

        final String coinbaseMessage = Constants.COINBASE_MESSAGE;

        final AddressInflater addressInflater = new AddressInflater();
        final Address address = addressInflater.compressedFromPrivateKey(_privateKey);

        final BlockHeader previousBlockHeader;
        {
            final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
            final NodeJsonRpcConnection nodeRpcConnection = _getNodeJsonRpcConnection();
            final Json blockHeadersResponseJson = nodeRpcConnection.getBlockHeaders(1, true);
            final Json blockHeadersJson = blockHeadersResponseJson.get("blockHeaders");
            previousBlockHeader = blockHeaderInflater.fromBytes(HexUtil.hexStringToByteArray(blockHeadersJson.getString(0)));
        }

        final Long blockHeight;
        {
            final NodeJsonRpcConnection nodeRpcConnection = _getNodeJsonRpcConnection();
            final Json blockHeightJson = nodeRpcConnection.getBlockHeight();
            blockHeight = (blockHeightJson.getLong("blockHeight") + 1L);
        }

        final Difficulty difficulty;
        {
            final NodeJsonRpcConnection nodeRpcConnection = _getNodeJsonRpcConnection();
            final Json difficultyJson = nodeRpcConnection.getDifficulty();
            difficulty = Difficulty.decode(HexUtil.hexStringToByteArray(difficultyJson.getString("difficulty")));
        }

        final Long blockReward;
        {
            final NodeJsonRpcConnection nodeRpcConnection = _getNodeJsonRpcConnection();
            final Json blockRewardJson = nodeRpcConnection.getBlockReward();
            blockReward = blockRewardJson.getLong("blockReward");
        }

        final Long totalTransactionFees;
        final List<Transaction> transactions;
        {
            Long transactionFees = 0L;

            final TransactionInflater transactionInflater = new TransactionInflater();
            final NodeJsonRpcConnection nodeRpcConnection = _getNodeJsonRpcConnection();
            final Json unconfirmedTransactionsResponseJson = nodeRpcConnection.getUnconfirmedTransactions(true);
            final Json unconfirmedTransactionsJson = unconfirmedTransactionsResponseJson.get("unconfirmedTransactions");

            final ImmutableListBuilder<Transaction> unconfirmedTransactionsListBuilder = new ImmutableListBuilder<Transaction>(unconfirmedTransactionsJson.length());

            for (int i = 0; i < unconfirmedTransactionsJson.length(); ++i) {
                final Json transactionWithFeeJsonObject = unconfirmedTransactionsJson.get(i);
                final String transactionData = transactionWithFeeJsonObject.getString("transaction");
                final Long transactionFee = transactionWithFeeJsonObject.getLong("transactionFee");

                final Transaction transaction = transactionInflater.fromBytes(HexUtil.hexStringToByteArray(transactionData));
                unconfirmedTransactionsListBuilder.add(transaction);
                transactionFees += transactionFee;
            }

            transactions = unconfirmedTransactionsListBuilder.build();
            totalTransactionFees = transactionFees;
        }

        final Transaction coinbaseTransaction = Transaction.createCoinbaseTransactionWithExtraNonce(blockHeight, coinbaseMessage, _totalExtraNonceByteCount, address,(blockReward + totalTransactionFees));

        stratumMineBlockTaskFactory.setBlockVersion(BlockHeader.VERSION);
        stratumMineBlockTaskFactory.setPreviousBlockHash(previousBlockHeader.getHash());
        stratumMineBlockTaskFactory.setDifficulty(difficulty);
        stratumMineBlockTaskFactory.setCoinbaseTransaction(coinbaseTransaction, _totalExtraNonceByteCount);
        stratumMineBlockTaskFactory.setExtraNonce(_extraNonce);

        for (final Transaction transaction : transactions) {
            stratumMineBlockTaskFactory.addTransaction(transaction);
        }

        try {
            _mineBlockTaskWriteLock.lock();

            _stratumMineBlockTaskFactory = stratumMineBlockTaskFactory;
            _currentMineBlockTask = stratumMineBlockTaskFactory.buildMineBlockTask();
            _mineBlockTasks.clear();
            _mineBlockTasks.put(_currentMineBlockTask.getId(), _currentMineBlockTask);

            _queuedTransactions.clear();
            _lastTransactionQueueProcessTimer.reset();
        }
        finally {
            _mineBlockTaskWriteLock.unlock();
        }
    }

    protected void _updateCurrentMiningTask() {
        try {
            _mineBlockTaskWriteLock.lock();

            final StratumMineBlockTask stratumMineBlockTask = _stratumMineBlockTaskFactory.buildMineBlockTask();
            _currentMineBlockTask = stratumMineBlockTask;
            _mineBlockTasks.put(stratumMineBlockTask.getId(), stratumMineBlockTask);
        }
        finally {
            _mineBlockTaskWriteLock.unlock();
        }
    }

    protected void _addQueuedTransactionsToCurrentMiningTask() {
        try {
            _mineBlockTaskWriteLock.lock();

            for (final Transaction queuedTransaction : _queuedTransactions) {
                _stratumMineBlockTaskFactory.addTransaction(queuedTransaction);
            }
            _queuedTransactions.clear();

            _lastTransactionQueueProcessTimer.reset();
            _lastTransactionQueueProcessTimer.start();
        }
        finally {
            _mineBlockTaskWriteLock.unlock();
        }
    }

    protected void _sendWork(final JsonSocket socketConnection, final Boolean abandonOldJobs) {
        _setDifficulty(socketConnection);

        final RequestMessage mineBlockRequest;
        try {
            _mineBlockTaskReadLock.lock();

            mineBlockRequest = _currentMineBlockTask.createRequest(abandonOldJobs);
        }
        finally {
            _mineBlockTaskReadLock.unlock();
        }

        Logger.log("Sent: "+ mineBlockRequest.toString());
        socketConnection.write(new JsonProtocolMessage(mineBlockRequest));
    }

    protected void _setDifficulty(final JsonSocket socketConnection) {
        final RequestMessage mineBlockMessage = new RequestMessage(RequestMessage.ServerCommand.SET_DIFFICULTY.getValue());

        final Json parametersJson = new Json(true);
        parametersJson.add(_shareDifficulty); // Difficulty::getDifficultyRatio
        mineBlockMessage.setParameters(parametersJson);

        Logger.log("Sent: "+ mineBlockMessage.toString());
        socketConnection.write(new JsonProtocolMessage(mineBlockMessage));
    }

    protected void _handleSubscribeMessage(final RequestMessage requestMessage, final JsonSocket socketConnection) {
        final String subscriptionId = HexUtil.toHexString(_createRandomBytes(8).getBytes());

        final Json resultJson = new Json(true); {
            final Json setDifficulty = new Json(true); {
                setDifficulty.add("mining.set_difficulty");
                setDifficulty.add(subscriptionId);
            }

            final Json notify = new Json(true); {
                notify.add("mining.notify");
                notify.add(subscriptionId);
            }

            final Json subscriptions = new Json(true); {
                subscriptions.add(setDifficulty);
                subscriptions.add(notify);
            }

            resultJson.add(subscriptions);
            try {
                _mineBlockTaskReadLock.lock();

                resultJson.add(_currentMineBlockTask.getExtraNonce());
            }
            finally {
                _mineBlockTaskReadLock.unlock();
            }
            resultJson.add(_extraNonce2ByteCount);
        }

        final ResponseMessage responseMessage = new ResponseMessage(requestMessage.getId());
        responseMessage.setResult(resultJson);

        Logger.log("Sent: "+ responseMessage);
        socketConnection.write(new JsonProtocolMessage(responseMessage));
    }

    protected void _handleAuthorizeMessage(final RequestMessage requestMessage, final JsonSocket socketConnection) {
        { // Respond with successful authorization...
            final ResponseMessage responseMessage = new ResponseMessage(requestMessage.getId());
            responseMessage.setResult(ResponseMessage.RESULT_TRUE);

            Logger.log("Sent: "+ responseMessage.toString());
            socketConnection.write(new JsonProtocolMessage(responseMessage));
        }

        _sendWork(socketConnection, true);
    }

    protected void _handleSubmitMessage(final RequestMessage requestMessage, final JsonSocket socketConnection) {
        // mining.submit("username", "job id", "ExtraNonce2", "nTime", "nOnce")

        final Json messageParameters = requestMessage.getParameters();
        final String workerName = messageParameters.getString(0);
        final ByteArray taskId = MutableByteArray.wrap(HexUtil.hexStringToByteArray(messageParameters.getString(1)));
        final String stratumNonce = messageParameters.getString(4);
        final String stratumExtraNonce2 = messageParameters.getString(2);
        final String stratumTimestamp = messageParameters.getString(3);

        Boolean submissionWasAccepted = true;

        final Long taskIdLong = ByteUtil.bytesToLong(taskId.getBytes());
        final StratumMineBlockTask mineBlockTask;
        try {
            _mineBlockTaskReadLock.lock();

            mineBlockTask = _mineBlockTasks.get(taskIdLong);
        }
        finally {
            _mineBlockTaskReadLock.unlock();
        }
        if (mineBlockTask == null) {
            submissionWasAccepted = false;
        }

        if (mineBlockTask != null) {
            final Difficulty shareDifficulty = Difficulty.BASE_DIFFICULTY.divideBy(_shareDifficulty);

            final BlockHeader blockHeader = mineBlockTask.assembleBlockHeader(stratumNonce, stratumExtraNonce2, stratumTimestamp);
            final Sha256Hash hash = blockHeader.getHash();
            Logger.log(mineBlockTask.getDifficulty().getBytes());
            Logger.log(hash);
            Logger.log(shareDifficulty.getBytes());

            if (! shareDifficulty.isSatisfiedBy(hash)) {
                submissionWasAccepted = false;
                Logger.log("NOTICE: Share Difficulty not satisfied.");

                final RequestMessage newRequestMessage = mineBlockTask.createRequest(false);
                Logger.log("Resending Task: "+ newRequestMessage.toString());
                socketConnection.write(new JsonProtocolMessage(newRequestMessage));
            }
            else if (blockHeader.isValid()) {
                final BlockHeaderDeflater blockHeaderDeflater = new BlockHeaderDeflater();
                Logger.log("Valid Block: " + blockHeaderDeflater.toBytes(blockHeader));

                final BlockDeflater blockDeflater = new BlockDeflater();
                final Block block = mineBlockTask.assembleBlock(stratumNonce, stratumExtraNonce2, stratumTimestamp);
                Logger.log(blockDeflater.toBytes(block));

                final NodeJsonRpcConnection nodeRpcConnection = _getNodeJsonRpcConnection();
                final Json submitBlockResponse = nodeRpcConnection.submitBlock(block);

                _rebuildNewMiningTask();
                _broadcastNewTask(true);
            }
        }

        final ResponseMessage blockAcceptedMessage = new MinerSubmitBlockResult(requestMessage.getId(), submissionWasAccepted);

        Logger.log("Sent: "+ blockAcceptedMessage.toString());
        socketConnection.write(new JsonProtocolMessage(blockAcceptedMessage));
    }

    public StratumModule(final String configurationFilename) {
        _configuration = _loadConfigurationFile(configurationFilename);

        final AddressInflater addressInflater = new AddressInflater();

        _privateKey = PrivateKey.createNewKey();
        Logger.log("Private Key: " + _privateKey);
        Logger.log("Address:     " + addressInflater.fromPrivateKey(_privateKey).toBase58CheckEncoded());

        _extraNonce = _createRandomBytes(_extraNonceByteCount);

        final Configuration.ServerProperties serverProperties = _configuration.getServerProperties();

        _stratumServerSocket = new StratumServerSocket(serverProperties.getStratumPort(), _threadPool);

        _stratumServerSocket.setSocketEventCallback(new StratumServerSocket.SocketEventCallback() {
            @Override
            public void onConnect(final JsonSocket socketConnection) {
                Logger.log("Node connected: " + socketConnection.getIp() + ":" + socketConnection.getPort());
                _connections.add(socketConnection);

                if (PROXY_VIABTC) {
                    try {
                        final JsonSocket viaBtcSocket = new JsonSocket(new Socket("bch.viabtc.com", 3333), _threadPool);

                        viaBtcSocket.setMessageReceivedCallback(new Runnable() {
                            @Override
                            public void run() {
                                final JsonProtocolMessage jsonProtocolMessage = viaBtcSocket.popMessage();
                                final Json message = jsonProtocolMessage.getMessage();
                                Logger.log("VIABTC SENT: " + message);

                                socketConnection.write(jsonProtocolMessage);
                            }
                        });

                        socketConnection.setMessageReceivedCallback(new Runnable() {
                            @Override
                            public void run() {
                                final JsonProtocolMessage jsonProtocolMessage = socketConnection.popMessage();
                                final Json message = jsonProtocolMessage.getMessage();
                                Logger.log("ASIC SENT: " + message);

                                viaBtcSocket.write(jsonProtocolMessage);
                            }
                        });

                        viaBtcSocket.beginListening();
                    }
                    catch (final Exception exception) {
                        Logger.log(exception);
                    }
                }
                else {
                    socketConnection.setMessageReceivedCallback(new Runnable() {
                        @Override
                        public void run() {
                            final JsonProtocolMessage jsonProtocolMessage = socketConnection.popMessage();
                            final Json message = jsonProtocolMessage.getMessage();

                            { // Handle Request Messages...
                                final RequestMessage requestMessage = RequestMessage.parse(message);
                                if (requestMessage != null) {
                                    Logger.log("Received: " + requestMessage);

                                    if (requestMessage.isCommand(RequestMessage.ClientCommand.SUBSCRIBE)) {
                                        _handleSubscribeMessage(requestMessage, socketConnection);
                                    }
                                    else if (requestMessage.isCommand(RequestMessage.ClientCommand.AUTHORIZE)) {
                                        _handleAuthorizeMessage(requestMessage, socketConnection);
                                    }
                                    else if (requestMessage.isCommand(RequestMessage.ClientCommand.SUBMIT)) {
                                        _handleSubmitMessage(requestMessage, socketConnection);
                                    }
                                    else {
                                        Logger.log("Unrecognized Message: " + requestMessage.getCommand());
                                    }
                                }
                            }

                            { // Handle Response Messages...
                                final ResponseMessage responseMessage = ResponseMessage.parse(message);
                                if (responseMessage != null) { System.out.println(responseMessage); }
                            }
                        }
                    });
                }

                socketConnection.beginListening();
            }

            @Override
            public void onDisconnect(final JsonSocket disconnectedSocket) {
                Logger.log("Node disconnected: " + disconnectedSocket.getIp() + ":" + disconnectedSocket.getPort());

                final Iterator<JsonSocket> iterator = _connections.iterator();
                while (iterator.hasNext()) {
                    final JsonSocket jsonSocket = iterator.next();
                    if (jsonSocket == null) { continue; }

                    if (Util.areEqual(disconnectedSocket, jsonSocket)) {
                        iterator.remove();
                        break;
                    }
                }
            }
        });

        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        _mineBlockTaskWriteLock = readWriteLock.writeLock();
        _mineBlockTaskReadLock = readWriteLock.readLock();
    }

    public void loop() {
        _rebuildNewMiningTask();

        final NodeJsonRpcConnection nodeAnnouncementsRpcConnection = _getNodeJsonRpcConnection();
        nodeAnnouncementsRpcConnection.upgradeToAnnouncementHook(new NodeJsonRpcConnection.RawAnnouncementHookCallback() {
            @Override
            public void onNewBlockHeader(final BlockHeader blockHeader) {
                Logger.log("New Block Received: " + blockHeader.getHash());
                _rebuildNewMiningTask();
                _broadcastNewTask(true);
            }

            @Override
            public void onNewTransaction(final Transaction transaction) {
                Logger.log("Adding Transaction: " + transaction.getHash());

                try {
                    _mineBlockTaskWriteLock.lock();

                    _queuedTransactions.add(transaction);

                    final Long msSinceLastTaskUpdate = _lastTransactionQueueProcessTimer.getMillisecondsElapsed();

                    if (msSinceLastTaskUpdate >= 1000) {
                        _addQueuedTransactionsToCurrentMiningTask();
                        _updateCurrentMiningTask();
                        _broadcastNewTask(false);
                    }
                }
                finally {
                    _mineBlockTaskWriteLock.unlock();
                }
            }
        });

        _stratumServerSocket.start();

        Logger.log("[Server Online]");

        while (true) {
            try {
                Thread.sleep(60000);

                _addQueuedTransactionsToCurrentMiningTask();
                _updateCurrentMiningTask();
                _broadcastNewTask(false);
            }
            catch (final InterruptedException exception) {
                break;
            }
        }
    }
}