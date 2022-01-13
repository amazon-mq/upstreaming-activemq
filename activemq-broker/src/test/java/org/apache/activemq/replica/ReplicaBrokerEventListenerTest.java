package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaBrokerEventListenerTest {

    private final Broker broker = mock(Broker.class);
    private final ActiveMQQueue testQueue = new ActiveMQQueue("TEST.QUEUE");
    private final Destination destinationQueue = mock(Queue.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final ReplicaBrokerSubscriptionHandler subscriptionHandler = mock(ReplicaBrokerSubscriptionHandler.class);

    private final ReplicaBrokerEventListener listener = new ReplicaBrokerEventListener(broker, subscriptionHandler);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    @Before
    public void setUp() throws Exception {
        when(broker.getAdminConnectionContext()).thenReturn(connectionContext);
        when(broker.getDestinations(any())).thenReturn(Set.of(destinationQueue));
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

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker).getAdminConnectionContext();
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());

        ActiveMQMessage value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(message);

        verify(connectionContext).isProducerFlowControl();
        verify(connectionContext).setProducerFlowControl(false);
        verify(connectionContext).setProducerFlowControl(true);

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_ACK() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_CONSUMED() throws Exception {
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testQueue);
        final MessageAck ack = new MessageAck(message, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_CONSUMED)
                .setEventData(eventSerializer.serializeReplicationData(ack));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setContent(event.getEventData());

        listener.onMessage(replicaEventMessage);

        verify((Queue) destinationQueue, times(1)).removeMessage(messageId.toString());

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_DISCARDED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_DROPPED() throws Exception {
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testQueue);
        final MessageAck ack = new MessageAck(message, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_DROPPED)
                .setEventData(eventSerializer.serializeReplicationData(ack));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setContent(event.getEventData());

        listener.onMessage(replicaEventMessage);

        verify((Queue) destinationQueue, times(1)).removeMessage(messageId.toString());

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_BEGIN() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).beginTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_PREPARE() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).prepareTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_ROLLBACK() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).rollbackTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_COMMIT() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_COMMIT)
                .setEventData(eventSerializer.serializeReplicationData(transactionId))
                .setReplicationProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY, true);
        message.setContent(event.getEventData());
        message.setProperties(event.getReplicationProperties());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        ArgumentCaptor<Boolean> onePhaseArgumentCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(broker).commitTransaction(any(), messageArgumentCaptor.capture(), onePhaseArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        Boolean onePhase = onePhaseArgumentCaptor.getValue();
        assertThat(onePhase).isTrue();
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_FORGET() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).forgetTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
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
