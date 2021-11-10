package org.apache.activemq.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.Test;

public class ReplicaBrokerEventListenerTest {

    private final Broker broker = mock(Broker.class);
    private final ReplicaBrokerSubscriptionHandler subscriptionHandler = mock(ReplicaBrokerSubscriptionHandler.class);

    private final ReplicaBrokerEventListener listener = new ReplicaBrokerEventListener(broker, subscriptionHandler);

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_SEND() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_ACK() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }
    @Test
    public void canHandleEventOfType_MESSAGE_CONSUMED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_DISCARDED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_BEGIN() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_PREPARE() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_ROLLBACK() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_COMMIT() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_FORGET() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_EXPIRED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_SUBSCRIBER_REMOVED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_SUBSCRIBER_ADDED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

}
