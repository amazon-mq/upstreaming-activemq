package org.apache.activemq.replica;

import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicaEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaEventSerializer.class);
    private final WireFormat wireFormat = new OpenWireFormatFactory().createWireFormat();

    private final AtomicLong serializeReplicationDataExecutionCounts = new AtomicLong();
    private final AtomicLong serializeReplicationDataTotalExecutionTime = new AtomicLong();
    private final AtomicLong serializeMessageDataExecutionCounts = new AtomicLong();
    private final AtomicLong serializeMessageDataTotalExecutionTime = new AtomicLong();
    private final AtomicLong deserializeExecutionCounts = new AtomicLong();
    private final AtomicLong deserializeTotalExecutionTime = new AtomicLong();

    byte[] serializeReplicationData(final DataStructure object) throws IOException {
        try {
            final long startNano = System.nanoTime();
            ByteSequence packet = wireFormat.marshal(object);
            final byte[] bytes = ByteSequenceData.toByteArray(packet);
            long serializeCurrentExecution = serializeReplicationDataExecutionCounts.addAndGet(1);
            long serializeCurrentTotalTime = serializeReplicationDataTotalExecutionTime.addAndGet(System.nanoTime() - startNano);
            if (serializeCurrentExecution % 1000 == 0) {
                logger.error("[serializeReplicationData] average execution time for [{}] messages is [{}]ns", serializeCurrentExecution, serializeCurrentTotalTime / serializeCurrentExecution);
            }
            return bytes;
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize data: " + object.toString() + " in container: " + e, e);
        }
    }

    byte[] serializeMessageData(final Message message) throws IOException {
        try {
            final long startNano = System.nanoTime();
            ByteSequence packet = wireFormat.marshal(message);
            final byte[] bytes = ByteSequenceData.toByteArray(packet);
            long serializeMessageDataCurrentExecution = serializeMessageDataExecutionCounts.addAndGet(1);
            long serializeMessageDataCurrentTotalTime = serializeMessageDataTotalExecutionTime.addAndGet(System.nanoTime() - startNano);
            if (serializeMessageDataCurrentExecution % 1000 == 0) {
                logger.error("[serializeMessageData] average execution time for [{}] messages is [{}]ns", serializeMessageDataCurrentExecution, serializeMessageDataCurrentTotalTime / serializeMessageDataCurrentExecution);
            }
            return bytes;
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize message: " + message.getMessageId() + " in container: " + e, e);
        }
    }

    Object deserializeMessageData(final ByteSequence sequence) throws IOException {
        final long startNano = System.nanoTime();
        final Object unmarshal = wireFormat.unmarshal(sequence);
        long deserializeCurrentExecution = deserializeExecutionCounts.addAndGet(1);
        long deserializeCurrentTotalTime = deserializeTotalExecutionTime.addAndGet(System.nanoTime() - startNano);
        if (deserializeCurrentExecution % 1000 == 0) {
            logger.error("[serializeMessageData] average execution time for [{}] messages is [{}]ns", deserializeCurrentExecution, deserializeCurrentTotalTime / deserializeCurrentExecution);
        }
        return unmarshal;
    }
}
