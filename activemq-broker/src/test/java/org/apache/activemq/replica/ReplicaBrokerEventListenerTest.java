package org.apache.activemq.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReplicaBrokerEventListenerTest {

    private final Broker broker = mock(Broker.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final ReplicaBrokerSubscriptionHandler subscriptionHandler = mock(ReplicaBrokerSubscriptionHandler.class);

    private final ReplicaBrokerEventListener listener = new ReplicaBrokerEventListener(broker, subscriptionHandler);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    @Before
    public void setUp() {
        when(broker.getAdminConnectionContext()).thenReturn(connectionContext);
        when(connectionContext.isProducerFlowControl()).thenReturn(true);
    }

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
    public void canHandleEventOfType_MESSAGE_SEND() throws Exception {
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ActiveMQMessage replicaMessage = new ActiveMQMessage();

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        replicaMessage.setContent(event.getEventData());
        replicaMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaMessage);

        verify(broker).getAdminConnectionContext();
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());

        ActiveMQMessage value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(message);

        verify(connectionContext).isProducerFlowControl();
        verify(connectionContext).setProducerFlowControl(false);
        verify(connectionContext).setProducerFlowControl(true);

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
