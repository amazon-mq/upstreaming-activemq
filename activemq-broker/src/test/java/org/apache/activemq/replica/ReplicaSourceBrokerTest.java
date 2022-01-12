package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.DestinationMapEntry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaSourceBrokerTest {

    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<Boolean>() {};
    private final Broker broker = mock(Broker.class);
    private final BrokerService brokerService = mock(BrokerService.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);

    private final ReplicaSourceBroker source = new ReplicaSourceBroker(broker);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final ActiveMQQueue testDestination = new ActiveMQQueue("TEST.QUEUE");

    @Before
    public void setUp() throws Exception {
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(broker.getAdminConnectionContext()).thenReturn(connectionContext);
        when(brokerService.getBroker()).thenReturn(source);
        when(brokerService.getDestinationInterceptors()).thenReturn(new DestinationInterceptor[] {});
        when(connectionContext.isProducerFlowControl()).thenReturn(true);

        source.destinationsToReplicate.put(testDestination, IS_REPLICATED);
    }

    @Test
    public void createsQueueOnInitialization() throws Exception {
        source.start();

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());

        ActiveMQDestination replicationDestination = destinationArgumentCaptor.getValue();
        assertThat(replicationDestination.getPhysicalName()).contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX);
    }

    @Test
    public void createsDestinationEventsOnStartup() throws Exception {
        doAnswer(invocation -> {
            source.addDestination(connectionContext, testDestination, true);
            return null;
        }).when(broker).start();

        Queue queue = mock(Queue.class);
        when(broker.addDestination(connectionContext, testDestination, true)).thenReturn(queue);

        source.start();

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(2)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());

        List<ActiveMQDestination> destinations = destinationArgumentCaptor.getAllValues();

        ActiveMQDestination replicationDestination = destinations.get(0);
        assertThat(replicationDestination.getPhysicalName()).contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX);

        ActiveMQDestination precreatedDestination = destinations.get(1);
        assertThat(precreatedDestination).isEqualTo(testDestination);
    }

    @Test
    public void doesNotCreateDestinationEventsForNonReplicabledDestinations() throws Exception {
        source.start();

        ActiveMQTopic advisoryTopic = new ActiveMQTopic(AdvisorySupport.ADVISORY_TOPIC_PREFIX + "TEST");
        source.addDestination(connectionContext, advisoryTopic, true);

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(2)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());


        List<ActiveMQDestination> destinations = destinationArgumentCaptor.getAllValues();

        ActiveMQDestination replicationDestination = destinations.get(0);
        assertThat(replicationDestination.getPhysicalName()).contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX);

        ActiveMQDestination advisoryTopicDestination = destinations.get(1);
        assertThat(advisoryTopicDestination).isEqualTo(advisoryTopic);

        verify(broker, never()).send(any(), any());
    }

    @Test
    public void replicates_ADD_ONE_TEST_FOR_EACH_TYPE_OF_REPLICATED_EVENT() {
        fail("Implement me");
    }

    @Test
    public void replicates_MESSAGE_SEND() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(connectionContext);

        source.send(producerExchange, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(2)).send(any(), messageArgumentCaptor.capture());

        final List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();

        ActiveMQMessage originalMessage = values.get(0);
        assertThat(originalMessage).isEqualTo(message);

        ActiveMQMessage replicaMessage = values.get(1);
        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.MESSAGE_SEND.name());
        assertThat(eventSerializer.deserializeMessageData(replicaMessage.getContent())).isEqualTo(message);

        verifyConnectionContext(connectionContext);
    }

    @Test
    public void doesNotReplicateAdvisoryTopics() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);

        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(connectionContext);

        source.send(producerExchange, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());

        final List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();

        ActiveMQMessage originalMessage = values.get(0);
        assertThat(originalMessage).isEqualTo(message);

        verify(connectionContext, never()).isProducerFlowControl();
        verify(connectionContext, never()).setProducerFlowControl(anyBoolean());
    }

    @Test
    public void replicates_MESSAGE_CONSUMED() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testDestination);

        source.messageConsumed(connectionContext, message);

        ArgumentCaptor<ActiveMQMessage> consumeMessageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).messageConsumed(any(), consumeMessageArgumentCaptor.capture());
        ArgumentCaptor<ActiveMQMessage> sendMessageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), sendMessageArgumentCaptor.capture());
        ActiveMQMessage originalMessage = consumeMessageArgumentCaptor.getValue();
        assertThat(originalMessage).isEqualTo(message);
        ActiveMQMessage replicaMessage = sendMessageArgumentCaptor.getValue();
        final MessageAck ackMessage = (MessageAck) eventSerializer.deserializeMessageData(replicaMessage.getContent());
        assertThat(ackMessage.getLastMessageId()).isEqualTo(messageId);
        assertThat(ackMessage.getDestination()).isEqualTo(testDestination);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void addsListenerToQueueOnAddDestination() throws Exception {
        source.start();

        Queue queue = mock(Queue.class);
        when(broker.addDestination(connectionContext, testDestination, true)).thenReturn(queue);

        source.addDestination(connectionContext, testDestination, true);

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(2)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());

        List<ActiveMQDestination> destinations = destinationArgumentCaptor.getAllValues();

        ActiveMQDestination replicationDestination = destinations.get(0);
        assertThat(replicationDestination.getPhysicalName()).contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX);

        ActiveMQDestination destination = destinations.get(1);
        assertThat(destination).isEqualTo(testDestination);

        verify(queue).addListener(source);
    }

    @Test
    public void replicates_MESSAGE_DROPPED() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testDestination);

        source.onDropMessage(new IndirectMessageReference(message));

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicaMessage = messageArgumentCaptor.getValue();

        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.MESSAGE_DROPPED.name());

        final MessageAck ackMessage = (MessageAck) eventSerializer.deserializeMessageData(replicaMessage.getContent());
        assertThat(ackMessage.getLastMessageId()).isEqualTo(messageId);
        assertThat(ackMessage.getDestination()).isEqualTo(testDestination);
        verifyConnectionContext(connectionContext);
    }

    private void verifyConnectionContext(ConnectionContext context) {
        verify(context).isProducerFlowControl();
        verify(context).setProducerFlowControl(false);
        verify(context).setProducerFlowControl(true);
    }
}
