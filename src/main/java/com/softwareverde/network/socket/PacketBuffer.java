package com.softwareverde.network.socket;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.p2p.message.ProtocolMessage;
import com.softwareverde.network.p2p.message.ProtocolMessageFactory;
import com.softwareverde.network.p2p.message.ProtocolMessageHeader;
import com.softwareverde.network.p2p.message.ProtocolMessageHeaderInflater;
import com.softwareverde.util.ByteBuffer;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.HexUtil;
import com.softwareverde.util.Util;

public class PacketBuffer extends ByteBuffer {
    protected final int _mainNetMagicNumberByteCount;
    protected final byte[] _reversedMainNetMagicNumber;

    protected final ProtocolMessageHeaderInflater _protocolMessageHeaderInflater;
    protected final ProtocolMessageFactory _protocolMessageFactory;

    protected final byte[] _packetStartingBytesBuffer;

    protected ProtocolMessageHeader _peakProtocolHeader() {
        final int headerByteCount = _protocolMessageHeaderInflater.getHeaderByteCount();

        if (_byteCount < headerByteCount) { return null; }

        final byte[] packetHeader = _peakContiguousBytes(headerByteCount);
        return _protocolMessageHeaderInflater.fromBytes(packetHeader);
    }

    public PacketBuffer(final BinaryPacketFormat binaryPacketFormat) {
        final ByteArray magicNumber = binaryPacketFormat.getMagicNumber();
        final int magicNumberByteCount = magicNumber.getByteCount();
        _mainNetMagicNumberByteCount = magicNumberByteCount;
        _packetStartingBytesBuffer = new byte[magicNumberByteCount];
        _reversedMainNetMagicNumber = ByteUtil.reverseEndian(magicNumber.getBytes());

        _protocolMessageHeaderInflater = binaryPacketFormat.getProtocolMessageHeaderInflater();
        _protocolMessageFactory = binaryPacketFormat.getProtocolMessageFactory();
    }

    public boolean hasMessage() {
        final ProtocolMessageHeader protocolMessageHeader = _peakProtocolHeader();
        if (protocolMessageHeader == null) { return false; }
        final Integer expectedMessageLength = (protocolMessageHeader.getPayloadByteCount() + _protocolMessageHeaderInflater.getHeaderByteCount());
        return (_byteCount >= expectedMessageLength);
    }

    public ProtocolMessage popMessage() {
        final ProtocolMessageHeader protocolMessageHeader = _peakProtocolHeader();
        if (protocolMessageHeader == null) { return null; }

        final int headerByteCount  = _protocolMessageHeaderInflater.getHeaderByteCount();
        final int payloadByteCount = protocolMessageHeader.getPayloadByteCount();

        if (_byteCount < payloadByteCount) {
            Logger.debug("PacketBuffer.popMessage: Insufficient byte count.");
            return null;
        }

        final int fullPacketByteCount = (headerByteCount + payloadByteCount);

        final byte[] fullPacket = _consumeContiguousBytes(fullPacketByteCount);

        if (fullPacketByteCount > Util.coalesce(_protocolMessageHeaderInflater.getMaxPacketByteCount(protocolMessageHeader), Integer.MAX_VALUE)) {
            Logger.debug("Dropping packet. Packet exceeded max byte count: " + fullPacketByteCount);
            return null;
        }

        final ProtocolMessage protocolMessage = _protocolMessageFactory.fromBytes(fullPacket);
        if (protocolMessage == null) {
            Logger.debug("Error inflating message: " + HexUtil.toHexString(ByteUtil.copyBytes(fullPacket, 0, Math.min(fullPacket.length, 128))) + " (+"+ ( (fullPacket.length > 128) ? (fullPacket.length - 128) : 0 ) +" bytes)");
        }

        return protocolMessage;
    }
}