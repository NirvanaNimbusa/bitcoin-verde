package com.softwareverde.bitcoin.server.module.node.store.dat;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.MutableBlock;
import com.softwareverde.bitcoin.block.header.MutableBlockHeader;
import com.softwareverde.bitcoin.inflater.BlockHeaderInflaters;
import com.softwareverde.bitcoin.inflater.BlockInflaters;
import com.softwareverde.bitcoin.server.module.node.store.BlockStore;
import com.softwareverde.bitcoin.util.ByteBuffer;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.bitcoin.util.IoUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;

import java.io.File;
import java.io.RandomAccessFile;

public class BlkDatBlockStore implements BlockStore {
    public static final ByteArray BLOCK_MAGIC = ByteArray.fromHexString("F9BEB4D9");

    protected final BlockHeaderInflaters _blockHeaderInflaters;
    protected final BlockInflaters _blockInflaters;
    protected final String _blkDatDirectory;
    protected final Integer _blocksPerDirectoryCount = 2016; // About 2 weeks...
    protected final BlkDatIndex _blkDatIndex;

    protected final ByteBuffer _byteBuffer = new ByteBuffer();

    protected ByteArray _readFromBlock(final String blockPath, final Long diskOffset, final Integer byteCount) {
        if (_blkDatDirectory == null) { return null; }

        if (blockPath == null) { return null; }

        if (! IoUtil.fileExists(blockPath)) { return null; }

        try {
            final MutableByteArray byteArray = new MutableByteArray(byteCount);

            try (final RandomAccessFile file = new RandomAccessFile(new File(blockPath), "r")) {
                file.seek(diskOffset);

                final byte[] buffer;
                synchronized (_byteBuffer) {
                    buffer = _byteBuffer.getRecycledBuffer();
                }

                int totalBytesRead = 0;
                while (totalBytesRead < byteCount) {
                    final int byteCountRead = file.read(buffer);
                    if (byteCountRead < 0) { break; }

                    byteArray.setBytes(totalBytesRead, buffer);
                    totalBytesRead += byteCountRead;
                }

                synchronized (_byteBuffer) {
                    _byteBuffer.recycleBuffer(buffer);
                }

                if (totalBytesRead < byteCount) { return null; }
            }

            return byteArray;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    public BlkDatBlockStore(final String blkDatDirectory, final BlkDatIndex blkDatIndex, final BlockHeaderInflaters blockHeaderInflaters, final BlockInflaters blockInflaters) {
        _blkDatDirectory = (blkDatDirectory + (blkDatDirectory.endsWith(File.separator) ? "" : File.separator));
        _blkDatIndex = blkDatIndex;
        _blockInflaters = blockInflaters;
        _blockHeaderInflaters = blockHeaderInflaters;
    }

    @Override
    public Boolean storeBlock(final Block block, final Long blockHeight) {
        return null;
    }

    @Override
    public void removeBlock(final Sha256Hash blockHash, final Long blockHeight) { }

    @Override
    public MutableBlockHeader getBlockHeader(final Sha256Hash blockHash, final Long blockHeight) {
        return null;
    }

    @Override
    public MutableBlock getBlock(final Sha256Hash blockHash, final Long blockHeight) {
        if (_blkDatDirectory == null) { return null; }

        final BlkDatIndex.FilePointer filePointer = _blkDatIndex.getFilePointer(blockHash, blockHeight);
        final String blockPath = (_blkDatDirectory + filePointer.blkDatFile);

        final long blockMetaDataIndex = filePointer.fileOffset;
        final long blockSizeIndex = (blockMetaDataIndex + BLOCK_MAGIC.getByteCount());
        final ByteArray blockByteCountBytes = _readFromBlock(blockPath, blockSizeIndex, 4);
        final Integer blockByteCount = (int) ByteUtil.bytesToLong(blockByteCountBytes.toReverseEndian());

        final Long blockDataIndex = (blockSizeIndex + blockByteCountBytes.getByteCount());
        final ByteArray blockBytes = _readFromBlock(blockPath, blockDataIndex, blockByteCount);
        if (blockBytes == null) { return null; }

        final BlockInflater blockInflater = _blockInflaters.getBlockInflater();
        return blockInflater.fromBytes(blockBytes);
    }

    @Override
    public ByteArray readFromBlock(final Sha256Hash blockHash, final Long blockHeight, final Long diskOffset, final Integer byteCount) {
        return null;
    }
}
