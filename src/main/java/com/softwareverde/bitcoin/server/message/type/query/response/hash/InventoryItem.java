package com.softwareverde.bitcoin.server.message.type.query.response.hash;

import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.bytearray.ByteArrayBuilder;
import com.softwareverde.util.bytearray.Endian;

public class InventoryItem {
    protected final InventoryItemType _inventoryItemType;
    protected final Sha256Hash _objectHash;

    public InventoryItem(final InventoryItemType inventoryItemType, final Sha256Hash objectHash) {
        _inventoryItemType = inventoryItemType;
        _objectHash = objectHash.asConst();
    }

    public InventoryItemType getItemType() {
        return _inventoryItemType;
    }

    public Sha256Hash getItemHash() {
        return _objectHash;
    }

    public byte[] getBytes() {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();

        final byte[] inventoryTypeBytes = new byte[4];
        ByteUtil.setBytes(inventoryTypeBytes, ByteUtil.integerToBytes(_inventoryItemType.getValue()));

        byteArrayBuilder.appendBytes(inventoryTypeBytes, Endian.LITTLE);
        byteArrayBuilder.appendBytes(_objectHash.getBytes(), Endian.LITTLE);

        return byteArrayBuilder.build();
    }

    @Override
    public String toString() {
        return (_inventoryItemType + ":" + _objectHash);
    }

    @Override
    public int hashCode() {
        return (_inventoryItemType.hashCode() + _objectHash.hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        if (! (object instanceof InventoryItem)) { return false; }

        final InventoryItem inventoryItem = (InventoryItem) object;
        if (! Util.areEqual(_inventoryItemType, inventoryItem.getItemType())) { return false; }

        return Util.areEqual(_objectHash, inventoryItem.getItemHash());
    }
}