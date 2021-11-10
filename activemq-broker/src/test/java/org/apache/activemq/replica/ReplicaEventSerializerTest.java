package org.apache.activemq.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;

public class ReplicaEventSerializerTest {

    private final ReplicaEventSerializer serializer = new ReplicaEventSerializer();

    @Test
    public void canDoRoundTripSerializedForDataOf_DESTINATION_UPSERT() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_DESTINATION_DELETE() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_MESSAGE_SEND() throws IOException {
        var message = new ActiveMQMessage();
        fail("Need correct data for test");

        var bytes = serializer.serializeMessageData(message);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(ActiveMQMessage.class)
            .isEqualTo(message);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_MESSAGE_ACK() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_MESSAGE_CONSUMED() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_MESSAGE_DISCARDED() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_BEGIN() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_PREPARE() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_ROLLBACK() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_COMMIT() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_FORGET() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_MESSAGE_EXPIRED() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_SUBSCRIBER_REMOVED() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    public void canDoRoundTripSerializedForDataOf_SUBSCRIBER_ADDED() throws IOException {
        var object = new Object();
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    private ByteSequence asSequence(byte[] bytes) {
        return new ByteSequence(bytes);
    }

}
