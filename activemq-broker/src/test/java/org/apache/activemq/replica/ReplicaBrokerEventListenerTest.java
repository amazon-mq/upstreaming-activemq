package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaBrokerEventListenerTest {

    private final Broker broker = mock(Broker.class);
    private final RegionBroker regionBroker = mock(RegionBroker.class);
    private final Region region = mock(Region.class);
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
        when(broker.getAdaptor(RegionBroker.class)).thenReturn(regionBroker);
        when(regionBroker.getRegion(testQueue)).thenReturn(region);
    }

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT_whenQueueNotExist() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
            .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker).addDestination(connectionContext, testQueue, true);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT_whenQueueExists() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination, testQueue});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
            .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).addDestination(connectionContext, testQueue, true);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE_whenDestinationExists() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination, testQueue});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.DESTINATION_DELETE)
            .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker).removeDestination(connectionContext, testQueue, 1000);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE_whenDestinationNotExists() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.DESTINATION_DELETE)
            .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).removeDestination(connectionContext, testQueue, 1000);
        verify(replicaEventMessage).acknowledge();
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
    public void canHandleEventOfType_MESSAGE_ACK_whenDestinationNotFound() throws Exception {
        when(broker.getDestinations(any())).thenReturn(Collections.emptySet());
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testQueue);
        final MessageAck ack = spy(new MessageAck(message, MessageAck.INDIVIDUAL_ACK_TYPE, 1));
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.MESSAGE_ACK)
            .setEventData(eventSerializer.serializeReplicationData(ack));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setContent(event.getEventData());

        listener.onMessage(replicaEventMessage);
        verify(replicaEventMessage).acknowledge();
        verify(ack, never()).setConsumerId(any());
        verify(region, never()).acknowledge(any(), eq(ack));
    }

    @Test
    public void canHandleEventOfType_MESSAGE_ACK_whenDestinationExists() throws Exception {
        when(destinationQueue.getActiveMQDestination()).thenReturn(testQueue);
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testQueue);
        final MessageAck ack = new MessageAck(message, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.MESSAGE_ACK)
            .setEventData(eventSerializer.serializeReplicationData(ack));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setContent(event.getEventData());

        listener.onMessage(replicaEventMessage);

        verify(replicaEventMessage).acknowledge();
        ArgumentCaptor<ConsumerBrokerExchange> consumerBrokerExchangeArgumentCaptor = ArgumentCaptor.forClass(ConsumerBrokerExchange.class);
        verify(region).acknowledge(consumerBrokerExchangeArgumentCaptor.capture(), any());
        ConsumerBrokerExchange calledConsumerBrokerExchange = consumerBrokerExchangeArgumentCaptor.getValue();
        assertThat(calledConsumerBrokerExchange.getRegion()).isEqualTo(broker);
        assertThat(calledConsumerBrokerExchange.getRegionDestination()).isEqualTo(destinationQueue);
        assertThat(calledConsumerBrokerExchange.getConnectionContext()).isEqualTo(connectionContext);
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
    public void canHandleEventOfType_MESSAGE_DISCARDED() throws Exception {
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testQueue);
        final MessageAck ack = new MessageAck(message, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.MESSAGE_DISCARDED)
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
    public void canHandleEventOfType_MESSAGE_EXPIRED() throws Exception {
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testQueue);
        final MessageAck ack = new MessageAck(message, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.MESSAGE_EXPIRED)
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
    @Ignore
    public void canHandleEventOfType_SUBSCRIBER_REMOVED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }

    @Test
    @Ignore
    public void canHandleEventOfType_SUBSCRIBER_ADDED() {
        var message = new ActiveMQMessage();

        listener.onMessage(message);

        assertThat(Boolean.TRUE).withFailMessage("Needs implementation").isFalse();
    }
}
