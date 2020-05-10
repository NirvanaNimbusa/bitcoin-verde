package com.softwareverde.bitcoin.server.module.node.handler.block;

import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.bitcoin.server.message.type.query.block.QueryBlocksMessage;
import com.softwareverde.bitcoin.server.message.type.query.response.InventoryMessage;
import com.softwareverde.bitcoin.server.message.type.query.response.hash.InventoryItem;
import com.softwareverde.bitcoin.server.message.type.query.response.hash.InventoryItemType;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.node.BitcoinNode;
import com.softwareverde.constable.list.List;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.logging.Logger;

public class QueryBlocksHandler extends AbstractQueryBlocksHandler implements BitcoinNode.QueryBlocksCallback {
    public static final BitcoinNode.QueryBlocksCallback IGNORE_REQUESTS_HANDLER = new BitcoinNode.QueryBlocksCallback() {
        @Override
        public void run(final List<Sha256Hash> blockHashes, final Sha256Hash desiredBlockHash, final BitcoinNode bitcoinNode) { }
    };

    protected final DatabaseManagerFactory _databaseManagerFactory;

    public QueryBlocksHandler(final DatabaseManagerFactory databaseManagerFactory) {
        _databaseManagerFactory = databaseManagerFactory;
    }

    @Override
    public void run(final List<Sha256Hash> blockHashes, final Sha256Hash desiredBlockHash, final BitcoinNode bitcoinNode) {
        try (final DatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            final StartingBlock startingBlock = _getStartingBlock(blockHashes, true, desiredBlockHash, databaseManager);

            if (startingBlock == null) {
                Logger.debug("Unable to send blocks: No blocks available.");
                return;
            }

            Sha256Hash lastBlockHash = null;
            final InventoryMessage responseMessage = new InventoryMessage();
            {
                if (! startingBlock.matchWasFound) {
                    responseMessage.addInventoryItem(new InventoryItem(InventoryItemType.BLOCK, BlockHeader.GENESIS_BLOCK_HASH));
                    lastBlockHash = BlockHeader.GENESIS_BLOCK_HASH;
                }

                final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
                final List<BlockId> childrenBlockIds = _findBlockChildrenIds(startingBlock.startingBlockId, desiredBlockHash, startingBlock.selectedBlockchainSegmentId, QueryBlocksMessage.MAX_BLOCK_HASH_COUNT, databaseManager);
                for (final BlockId blockId : childrenBlockIds) {
                    final Sha256Hash blockHash = blockHeaderDatabaseManager.getBlockHash(blockId);
                    responseMessage.addInventoryItem(new InventoryItem(InventoryItemType.BLOCK, blockHash));
                    lastBlockHash = blockHash;
                }
            }

            bitcoinNode.setBatchContinueHash(lastBlockHash);

            { // Debug Logging...
                final Sha256Hash firstBlockHash = ((! blockHashes.isEmpty()) ? blockHashes.get(0) : null);
                final List<InventoryItem> responseHashes = responseMessage.getInventoryItems();
                final Sha256Hash responseHash = ((! responseHashes.isEmpty()) ? responseHashes.get(0).getItemHash() : null);
                Logger.debug("QueryBlocksHandler : " + bitcoinNode.getRemoteNodeIpAddress() + " " + firstBlockHash + " - " + desiredBlockHash + " -> " + responseHash);
            }

            bitcoinNode.queueMessage(responseMessage);
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
        }
    }
}