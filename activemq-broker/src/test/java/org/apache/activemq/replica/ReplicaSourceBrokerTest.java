package org.apache.activemq.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.stream.Stream;

public class ReplicaSourceBrokerTest {

    private final Broker broker = mock(Broker.class);
    private final BrokerService brokerService = mock(BrokerService.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);

    private final ReplicaSourceBroker source = new ReplicaSourceBroker(broker);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    @Before
    public void setUp() throws Exception {
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getBroker()).thenReturn(broker);
        when(brokerService.getDestinationInterceptors()).thenReturn(new DestinationInterceptor[] {});
        when(connectionContext.isProducerFlowControl()).thenReturn(true);

        source.start();
    }

    @Test
    public void createsQueueOnInitialization() {
        fail("Implement me");
    }

    @Test
    public void createsDestinationEventsOnStartup() {
        fail("Implement me");
    }

    @Test
    public void doesNotCreateDestinationEventsForNonReplicatedDestiantions() {
        fail("Implement me");
    }

    @Test
    public void replicates_ADD_ONE_TEST_FOR_EACH_TYPE_OF_REPLICATED_EVENT() {
        fail("Implement me");
    }

    @Test
    public void replicates_MESSAGE_SEND() throws Exception {
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

        verify(connectionContext).isProducerFlowControl();
        verify(connectionContext).setProducerFlowControl(false);
        verify(connectionContext).setProducerFlowControl(true);
    }

    @Test
    public void notReplicateAdvisoryTopics() throws Exception {
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
}
