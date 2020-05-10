package com.softwareverde.bitcoin.server.message;

import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.server.message.header.BitcoinProtocolMessageHeaderInflater;
import com.softwareverde.bitcoin.server.message.type.MessageType;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.network.p2p.message.ProtocolMessage;
import com.softwareverde.security.util.HashUtil;
import com.softwareverde.util.bytearray.ByteArrayBuilder;
import com.softwareverde.util.bytearray.Endian;

/**
 * Protocol Definition:
 *  https://bitcoin.org/en/developer-reference
 *  https://en.bitcoin.it/wiki/Protocol_documentation
 */

public abstract class BitcoinProtocolMessage implements ProtocolMessage {
    public static final BitcoinProtocolMessageFactory PROTOCOL_MESSAGE_FACTORY = new BitcoinProtocolMessageFactory();
    public static final BitcoinBinaryPacketFormat BINARY_PACKET_FORMAT = new BitcoinBinaryPacketFormat(ByteArray.fromHexString(BitcoinConstants.getNetMagicNumber()), PROTOCOL_MESSAGE_FACTORY.getProtocolMessageHeaderParser(), PROTOCOL_MESSAGE_FACTORY);

    protected static final Integer CHECKSUM_BYTE_COUNT = 4;

    public static ByteArray calculateChecksum(final ByteArray payload) {
        final ByteArray fullChecksum = HashUtil.doubleSha256(payload);
        final MutableByteArray checksum = new MutableByteArray(4);

        for (int i = 0; i< CHECKSUM_BYTE_COUNT; ++i) {
            checksum.setByte(i, fullChecksum.getByte(i));
        }

        return checksum;
    }

    protected final ByteArray _magicNumber;
    protected final MessageType _command;

    public BitcoinProtocolMessage(final MessageType command) {
        _magicNumber = BINARY_PACKET_FORMAT.getMagicNumber();
        _command = command;
    }

    private ByteArray _getBytes() {
        final ByteArray payload = _getPayload();

        final byte[] payloadSizeBytes = ByteUtil.integerToBytes(payload.getByteCount());
        final ByteArray checksum = BitcoinProtocolMessage.calculateChecksum(payload);

        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();

        byteArrayBuilder.appendBytes(_magicNumber, Endian.LITTLE);
        byteArrayBuilder.appendBytes(_command.getBytes(), Endian.BIG);
        byteArrayBuilder.appendBytes(payloadSizeBytes, Endian.LITTLE);
        byteArrayBuilder.appendBytes(checksum.getBytes(), Endian.BIG); // NOTICE: Bitcoin Cash wants the checksum to be big-endian.  Bitcoin Core documentation says little-endian.  Discovered via tcpdump on server.
        byteArrayBuilder.appendBytes(payload, Endian.BIG);

        return MutableByteArray.wrap(byteArrayBuilder.build());
    }

    protected abstract ByteArray _getPayload();

    public ByteArray getMagicNumber() {
        return _magicNumber;
    }

    public MessageType getCommand() {
        return _command;
    }

    public byte[] getHeaderBytes() {
        return ByteUtil.copyBytes(_getBytes().getBytes(), 0, BitcoinProtocolMessageHeaderInflater.HEADER_BYTE_COUNT);
    }

    @Override
    public ByteArray getBytes() {
        return _getBytes();
    }
}
