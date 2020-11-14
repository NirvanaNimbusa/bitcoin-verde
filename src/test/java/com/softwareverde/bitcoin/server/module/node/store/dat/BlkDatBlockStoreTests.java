package com.softwareverde.bitcoin.server.module.node.store.dat;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.inflater.BlockHeaderInflaters;
import com.softwareverde.bitcoin.inflater.BlockInflaters;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.test.UnitTest;
import com.softwareverde.constable.list.List;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.util.Tuple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class BlkDatBlockStoreTests extends UnitTest {
    @Override @Before
    public void before() throws Exception {
        super.before();
    }

    @Override @After
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void should_inflate_block() throws Exception {
        final MasterInflater masterInflater = new CoreInflater();
        final BlockHeaderInflaters blockHeaderInflaters = masterInflater;
        final BlockInflaters blockInflaters = masterInflater;

        final HashMap<Sha256Hash, BlkDatIndex.FilePointer> filePointerMap = new HashMap<>();

        final String datDirectory = (System.getProperty("user.dir") + "/src/test/resources/blkdat/");
        final BlkDatIndexer blkDatIndexer = new BlkDatIndexer(datDirectory, new BlkDatIndexer.Callback() {
            @Override
            public void onNewBlkDatIndices(final List<Tuple<Sha256Hash, BlkDatIndex.FilePointer>> filePointers) {
                for (final Tuple<Sha256Hash, BlkDatIndex.FilePointer> tuple : filePointers) {
                    final Sha256Hash blockHash = tuple.first;
                    final BlkDatIndex.FilePointer filePointer = tuple.second;

                    System.out.println(blockHash + " -> " + filePointer.blkDatFile + ":" + filePointer.fileOffset);
                    filePointerMap.put(blockHash, filePointer);
                }
            }
        }, blockHeaderInflaters.getBlockHeaderInflater());
        blkDatIndexer.run();

        final BlkDatIndex blkDatIndex = new BlkDatIndex() {
            @Override
            public FilePointer getFilePointer(final Sha256Hash blockHash, final Long blockHeight) {
                return filePointerMap.get(blockHash);
            }
        };

        final BlkDatBlockStore blkDatBlockStore = new BlkDatBlockStore(datDirectory, blkDatIndex, blockHeaderInflaters, blockInflaters);

        {
            final Sha256Hash blockHash = Sha256Hash.fromHexString("000000000019D6689C085AE165831E934FF763AE46A2A6C172B3F1B60A8CE26F");
            final Block block = blkDatBlockStore.getBlock(blockHash, null);
            Assert.assertEquals(blockHash, block.getHash());
        }

        {
            final Sha256Hash blockHash = Sha256Hash.fromHexString("000000000000000000961F8DF70261B77430D8EF826F1F7E168CD1850FD498BE");
            final Block block = blkDatBlockStore.getBlock(blockHash, null);
            Assert.assertEquals(blockHash, block.getHash());
        }

        {
            final Sha256Hash blockHash = Sha256Hash.fromHexString("0000000000000000002D5D4A1965F3C7787CF2B0A65B8335AEF7DB32819288B3");
            final Block block = blkDatBlockStore.getBlock(blockHash, null);
            Assert.assertEquals(blockHash, block.getHash());
        }

        {
            final Sha256Hash blockHash = Sha256Hash.fromHexString("00000000000000000138D3EED4BAF70D72DD67BFD5FBABBB60E0CBF64B97ACDA");
            final Block block = blkDatBlockStore.getBlock(blockHash, null);
            Assert.assertNull(block); // blk.dat is truncated mid-block
        }
    }
}
