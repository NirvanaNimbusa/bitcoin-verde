package com.softwareverde.bitcoin.server.module.node.rpc;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.concurrent.pool.ThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.json.Json;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.socket.JsonProtocolMessage;
import com.softwareverde.network.socket.JsonSocket;
import com.softwareverde.util.HexUtil;

public class NodeJsonRpcConnection implements AutoCloseable {
    public interface AnnouncementHookCallback {
        void onNewBlockHeader(Json blockHeaderJson);
        void onNewTransaction(Json transactionJson);
    }

    public interface RawAnnouncementHookCallback {
        void onNewBlockHeader(BlockHeader blockHeader);
        void onNewTransaction(Transaction transaction, Long fee);
    }

    public static final Long RPC_DURATION_TIMEOUT_MS = 30000L;

    protected final MasterInflater _masterInflater;
    protected final JsonSocket _jsonSocket;
    protected final Object _newMessageNotifier = new Object();

    protected final Runnable _onNewMessageCallback = new Runnable() {
        @Override
        public void run() {
            synchronized (_newMessageNotifier) {
                _newMessageNotifier.notifyAll();
            }
        }
    };

    protected Boolean _isUpgradedToHook = false;
    protected Boolean _announcementHookExpectsRawTransactionData = null;

    protected Json _executeJsonRequest(final Json rpcRequestJson) {
        if (_isUpgradedToHook) { throw new RuntimeException("Attempted to invoke Json request to a hook-upgraded socket."); }
        if (! _jsonSocket.isConnected()) { throw new RuntimeException("Attempted to invoke Json request to a closed socket."); }

        _jsonSocket.write(new JsonProtocolMessage(rpcRequestJson));
        _jsonSocket.beginListening();

        JsonProtocolMessage jsonProtocolMessage;
        synchronized (_newMessageNotifier) {
            jsonProtocolMessage = _jsonSocket.popMessage();
            if (jsonProtocolMessage == null) {
                try {
                    _newMessageNotifier.wait(RPC_DURATION_TIMEOUT_MS);
                }
                catch (final InterruptedException exception) { }

                jsonProtocolMessage = _jsonSocket.popMessage();
            }
        }

        return (jsonProtocolMessage != null ? jsonProtocolMessage.getMessage() : null);
    }

    protected Json _createRegisterHookRpcJson(final Boolean returnRawData, final Boolean includeTransactionFees, final List<Address> addressFilter) {
        final Json eventTypesJson = new Json(true);
        eventTypesJson.add("NEW_BLOCK");
        eventTypesJson.add("NEW_TRANSACTION");

        final Json parametersJson = new Json();
        parametersJson.put("events", eventTypesJson);
        parametersJson.put("rawFormat", (returnRawData ? 1 : 0));
        parametersJson.put("includeTransactionFees", (includeTransactionFees ? 1 : 0));

        if (addressFilter != null) {
            final Json addressFilterJson = new Json(true);
            for (final Address address : addressFilter) {
                final String addressString = address.toBase58CheckEncoded();
                addressFilterJson.add(addressString);
            }
            parametersJson.put("addressFilter", addressFilterJson);
        }

        final Json registerHookRpcJson = new Json();
        registerHookRpcJson.put("method", "POST");
        registerHookRpcJson.put("query", "ADD_HOOK");
        registerHookRpcJson.put("parameters", parametersJson);

        return registerHookRpcJson;
    }

    protected Json _getBlock(final Sha256Hash blockHash, final Boolean hexFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", blockHash);
        if (hexFormat != null) {
            rpcParametersJson.put("rawFormat", (hexFormat ? 1 : 0));
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    protected Json _getBlock(final Long blockHeight, final Boolean hexFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("blockHeight", blockHeight);
        if (hexFormat != null) {
            rpcParametersJson.put("rawFormat", (hexFormat ? 1 : 0));
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    protected Json _getBlockHeader(final Sha256Hash blockHash, final Boolean hexFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", blockHash);
        if (hexFormat != null) {
            rpcParametersJson.put("rawFormat", (hexFormat ? 1 : 0));
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_HEADER");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    protected Json _getBlockHeader(final Long blockHeight, final Boolean hexFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("blockHeight", blockHeight);
        if (hexFormat != null) {
            rpcParametersJson.put("rawFormat", (hexFormat ? 1 : 0));
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_HEADER");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    protected Json _getBlockTransactions(final Sha256Hash blockHash, final Integer pageSize, final Integer pageNumber) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", blockHash);
        if (pageSize != null) {
            rpcParametersJson.put("pageSize", pageSize);
        }
        if (pageNumber != null) {
            rpcParametersJson.put("pageNumber", pageNumber);
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_TRANSACTIONS");
        rpcRequestJson.put("parameters", rpcParametersJson);
        return _executeJsonRequest(rpcRequestJson);
    }

    protected Json _getBlockTransactions(final Long blockHeight, final Integer pageSize, final Integer pageNumber) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("blockHeight", blockHeight);
        if (pageSize != null) {
            rpcParametersJson.put("pageSize", pageSize);
        }
        if (pageNumber != null) {
            rpcParametersJson.put("pageNumber", pageNumber);
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_TRANSACTIONS");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    protected Json _getTransaction(final Sha256Hash transactionHash, final Boolean hexFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", transactionHash);
        if (hexFormat != null) {
            rpcParametersJson.put("rawFormat", (hexFormat ? 1 : 0));
        }

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "TRANSACTION");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public NodeJsonRpcConnection(final String hostname, final Integer port, final ThreadPool threadPool) {
        this(
            hostname,
            port,
            threadPool,
            new CoreInflater()
        );
    }

    public NodeJsonRpcConnection(final String hostname, final Integer port, final ThreadPool threadPool, final MasterInflater masterInflater) {
        _masterInflater = masterInflater;

        final java.net.Socket javaSocket;
        {
            java.net.Socket socket = null;
            try { socket = new java.net.Socket(hostname, port); }
            catch (final Exception exception) { Logger.debug(exception); }
            javaSocket = socket;
        }

        _jsonSocket = ((javaSocket != null) ? new JsonSocket(javaSocket, threadPool) : null);

        if (_jsonSocket != null) {
            _jsonSocket.setMessageReceivedCallback(_onNewMessageCallback);
        }
    }

    public NodeJsonRpcConnection(final java.net.Socket javaSocket, final ThreadPool threadPool) {
        this(
            javaSocket,
            threadPool,
            new CoreInflater()
        );
    }

    public NodeJsonRpcConnection(final java.net.Socket socket, final ThreadPool threadPool, final MasterInflater masterInflater) {
        _masterInflater = masterInflater;

        _jsonSocket = ((socket != null) ? new JsonSocket(socket, threadPool) : null);

        if (_jsonSocket != null) {
            _jsonSocket.setMessageReceivedCallback(_onNewMessageCallback);
        }
    }

    public Json getBlockHeaders(final Long blockHeight, final Integer maxBlockCount, final Boolean returnRawFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("blockHeight", blockHeight);
        rpcParametersJson.put("maxBlockCount", maxBlockCount);
        rpcParametersJson.put("rawFormat", (returnRawFormat ? 1 : 0));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_HEADERS");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getBlockHeaders(final Integer maxBlockCount, final Boolean returnRawFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("maxBlockCount", maxBlockCount);
        rpcParametersJson.put("rawFormat", (returnRawFormat ? 1 : 0));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_HEADERS");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getDifficulty() {
        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "DIFFICULTY");

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getBlockReward() {
        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_REWARD");

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getUnconfirmedTransactions(final Boolean returnRawFormat) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("rawFormat", (returnRawFormat ? 1 : 0));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "UNCONFIRMED_TRANSACTIONS");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getBlockHeight() {
        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_HEIGHT");

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getBlockHeaderHeight(final Sha256Hash blockHash) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", blockHash.toString());

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "BLOCK_HEIGHT");

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getBlockchainMetadata() {
        final Json rpcRequestJson = new Json();
        {
            rpcRequestJson.put("method", "GET");
            rpcRequestJson.put("query", "BLOCKCHAIN");
        }

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getNodes() {
        final Json rpcRequestJson = new Json();
        {
            rpcRequestJson.put("method", "GET");
            rpcRequestJson.put("query", "NODES");
        }

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getAddressTransactions(final Address address) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("address", address.toBase58CheckEncoded());

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "ADDRESS");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getBlock(final Sha256Hash blockHash) {
        return _getBlock(blockHash, null);
    }

    public Json getBlock(final Sha256Hash blockHash, final Boolean hexFormat) {
        return _getBlock(blockHash, hexFormat);
    }

    public Json getBlock(final Long blockHeight) {
        return _getBlock(blockHeight, null);
    }

    public Json getBlock(final Long blockHeight, final Boolean hexFormat) {
        return _getBlock(blockHeight, hexFormat);
    }

    public Json getBlockHeader(final Sha256Hash blockHash) {
        return _getBlockHeader(blockHash, null);
    }

    public Json getBlockHeader(final Sha256Hash blockHash, final Boolean hexFormat) {
        return _getBlockHeader(blockHash, hexFormat);
    }

    public Json getBlockHeader(final Long blockHeight) {
        return _getBlockHeader(blockHeight, null);
    }

    public Json getBlockHeader(final Long blockHeight, final Boolean hexFormat) {
        return _getBlockHeader(blockHeight, hexFormat);
    }

    public Json getBlockTransactions(final Sha256Hash blockHash, final Integer pageSize, final Integer pageNumber) {
        return _getBlockTransactions(blockHash, pageSize, pageNumber);
    }

    public Json getBlockTransactions(final Long blockHeight, final Integer pageSize, final Integer pageNumber) {
        return _getBlockTransactions(blockHeight, pageSize, pageNumber);
    }

    public Json getTransaction(final Sha256Hash transactionHash) {
        return _getTransaction(transactionHash, null);
    }

    public Json getTransaction(final Sha256Hash transactionHash, final Boolean hexFormat) {
        return _getTransaction(transactionHash, hexFormat);
    }

    public Json getStatus() {
        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "STATUS");

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getUtxoCacheStatus() {
        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "UTXO_CACHE");

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json commitUtxoCache() {
        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "POST");
        rpcRequestJson.put("query", "COMMIT_UTXO_CACHE");

        return _executeJsonRequest(rpcRequestJson);
    }

    /**
     * Subscribes to the Node for new Block/Transaction announcements.
     *  The NodeJsonRpcConnection is consumed by this operation and cannot be used for additional API calls.
     *  The underlying JsonSocket remains connected and must be closed when announcements are no longer desired.
     */
    public Boolean upgradeToAnnouncementHook(final AnnouncementHookCallback announcementHookCallback) {
        return this.upgradeToAnnouncementHook(announcementHookCallback, null);
    }
    public Boolean upgradeToAnnouncementHook(final AnnouncementHookCallback announcementHookCallback, final List<Address> addressesFilter) {
        if (announcementHookCallback == null) { throw new NullPointerException("Attempted to create AnnouncementHook without a callback."); }

        final Json registerHookRpcJson = _createRegisterHookRpcJson(false, true, addressesFilter);

        final Json upgradeResponseJson = _executeJsonRequest(registerHookRpcJson);
        if (! upgradeResponseJson.getBoolean("wasSuccess")) { return false; }

        _jsonSocket.setMessageReceivedCallback(new Runnable() {
            @Override
            public void run() {
                final JsonProtocolMessage message = _jsonSocket.popMessage();
                final Json json = message.getMessage();

                final String objectType = json.getString("objectType");
                final Json object = json.get("object");
                switch (objectType) {
                    case "BLOCK": {
                        announcementHookCallback.onNewBlockHeader(object);
                    } break;

                    case "TRANSACTION": {
                        announcementHookCallback.onNewTransaction(object);
                    } break;

                    default: { } break;
                }
            }
        });

        _isUpgradedToHook = true;
        _announcementHookExpectsRawTransactionData = false;

        return true;
    }

    public Boolean replaceAnnouncementHookAddressFilter(final List<Address> addressesFilter) {
        if (! _isUpgradedToHook) { return false; }

        final boolean returnRawData = _announcementHookExpectsRawTransactionData;
        final Json rpcRequestJson = _createRegisterHookRpcJson(returnRawData, true, addressesFilter);
        rpcRequestJson.put("query", "UPDATE_HOOK");
        _jsonSocket.write(new JsonProtocolMessage(rpcRequestJson));
        return true;
    }

    public Boolean upgradeToAnnouncementHook(final RawAnnouncementHookCallback announcementHookCallback) {
        return this.upgradeToAnnouncementHook(announcementHookCallback, null);
    }
    public Boolean upgradeToAnnouncementHook(final RawAnnouncementHookCallback announcementHookCallback, final List<Address> addressesFilter) {
        if (announcementHookCallback == null) { throw new NullPointerException("Null AnnouncementHookCallback found."); }

        final Json registerHookRpcJson = _createRegisterHookRpcJson(true, true, addressesFilter);

        final Json upgradeResponseJson = _executeJsonRequest(registerHookRpcJson);
        if (! upgradeResponseJson.getBoolean("wasSuccess")) { return false; }

        _jsonSocket.setMessageReceivedCallback(new Runnable() {
            @Override
            public void run() {
                final JsonProtocolMessage message = _jsonSocket.popMessage();
                final Json json = message.getMessage();

                final String objectType = json.getString("objectType");

                switch (objectType) {
                    case "BLOCK": {
                        final String objectData = json.getString("object");
                        final BlockHeaderInflater blockHeaderInflater = _masterInflater.getBlockHeaderInflater();
                        final BlockHeader blockHeader = blockHeaderInflater.fromBytes(HexUtil.hexStringToByteArray(objectData));
                        if (blockHeader == null) {
                            Logger.warn("Error inflating block: " + objectData);
                            return;
                        }

                        announcementHookCallback.onNewBlockHeader(blockHeader);
                    } break;

                    case "TRANSACTION": {
                        final String objectData = json.getString("object");
                        final TransactionInflater transactionInflater = _masterInflater.getTransactionInflater();
                        final Transaction transaction = transactionInflater.fromBytes(HexUtil.hexStringToByteArray(objectData));
                        if (transaction == null) {
                            Logger.warn("Error inflating transaction: " + objectData);
                            return;
                        }

                        announcementHookCallback.onNewTransaction(transaction, null);
                    } break;

                    case "TRANSACTION_WITH_FEE": {
                        final Json object = json.get("object");
                        final String transactionData = object.getString("transactionData");
                        final Long fee = object.getLong("transactionFee");
                        final TransactionInflater transactionInflater = _masterInflater.getTransactionInflater();
                        final Transaction transaction = transactionInflater.fromBytes(HexUtil.hexStringToByteArray(transactionData));
                        if (transaction == null) {
                            Logger.warn("Error inflating transaction: " + transactionData);
                            return;
                        }

                        announcementHookCallback.onNewTransaction(transaction, fee);
                    } break;

                    default: { } break;
                }
            }
        });

        _isUpgradedToHook = true;
        _announcementHookExpectsRawTransactionData = true;

        return true;
    }

    public Json validatePrototypeBlock(final Block block) {
        final Json rpcParametersJson = new Json();
        final BlockDeflater blockDeflater = _masterInflater.getBlockDeflater();
        rpcParametersJson.put("blockData", blockDeflater.toBytes(block));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "POST");
        rpcRequestJson.put("query", "VALIDATE_PROTOTYPE_BLOCK");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json submitTransaction(final Transaction transaction) {
        final Json rpcParametersJson = new Json();
        final TransactionDeflater transactionDeflater = _masterInflater.getTransactionDeflater();
        rpcParametersJson.put("transactionData", transactionDeflater.toBytes(transaction));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "POST");
        rpcRequestJson.put("query", "TRANSACTION");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json submitBlock(final Block block) {
        final Json rpcParametersJson = new Json();
        final BlockDeflater blockDeflater = _masterInflater.getBlockDeflater();
        rpcParametersJson.put("blockData", blockDeflater.toBytes(block));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "POST");
        rpcRequestJson.put("query", "BLOCK");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json isSlpTransaction(final Sha256Hash transactionHash) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", transactionHash);

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "IS_SLP_TRANSACTION");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json isValidSlpTransaction(final Sha256Hash transactionHash) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", transactionHash);

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "IS_VALID_SLP_TRANSACTION");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json validateTransaction(final Transaction transaction, final Boolean enableSlpValidation) {
        final TransactionDeflater transactionDeflater = new TransactionDeflater();
        final ByteArray transactionBytes = transactionDeflater.toBytes(transaction);

        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("transactionData", transactionBytes);
        rpcParametersJson.put("enableSlpValidation", (enableSlpValidation ? 1 : 0));

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "POST");
        rpcRequestJson.put("query", "IS_VALID_SLP_TRANSACTION");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public Json getSlpTokenId(final Sha256Hash transactionHash) {
        final Json rpcParametersJson = new Json();
        rpcParametersJson.put("hash", transactionHash);

        final Json rpcRequestJson = new Json();
        rpcRequestJson.put("method", "GET");
        rpcRequestJson.put("query", "SLP_TOKEN_ID");
        rpcRequestJson.put("parameters", rpcParametersJson);

        return _executeJsonRequest(rpcRequestJson);
    }

    public JsonSocket getJsonSocket() {
        return _jsonSocket;
    }

    @Override
    public void close() {
        _jsonSocket.close();
    }
}
