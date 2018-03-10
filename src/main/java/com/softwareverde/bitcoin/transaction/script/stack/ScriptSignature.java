package com.softwareverde.bitcoin.transaction.script.stack;

import com.softwareverde.bitcoin.secp256k1.signature.Signature;

public class ScriptSignature {
    public enum HashType {
        SIGNATURE_HASH_ALL(0x01),
        SIGNATURE_HASH_NONE(0x02),
        SIGNATURE_HASH_SINGLE(0x03),
        SIGNATURE_HASH_ANYONE_CAN_PAY(0x80);

        protected final byte _value;
        HashType(final Integer value) {
            _value = (byte) value.intValue();
        }

        public static HashType fromByte(final byte b) {
            for (final HashType hashType : HashType.values()) {
                if (hashType._value == b) { return hashType; }
            }

            return null;
        }

        public byte getValue() {
            return _value;
        }
    }

    public static ScriptSignature fromBytes(final byte[] bytes) {
        final Signature ecdsaSignature = Signature.fromBytes(bytes);
        if (ecdsaSignature == null) { return null; }

        final byte hashTypeByte = bytes[bytes.length - 1];
        final HashType hashType = HashType.fromByte(hashTypeByte);
        if (hashType == null) { return null; }

        return new ScriptSignature(ecdsaSignature, hashType);
    }

    protected final HashType _hashType;
    protected final Signature _signature;

    public ScriptSignature(final Signature signature, final HashType hashType) {
        _signature = signature;
        _hashType = hashType;
    }

    public HashType getHashType() {
        return _hashType;
    }

    public Signature getSignature() {
        return _signature;
    }
}