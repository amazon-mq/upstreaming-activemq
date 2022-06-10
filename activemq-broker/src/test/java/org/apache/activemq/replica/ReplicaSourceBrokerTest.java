package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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

    private final Broker broker = mock(Broker.class);
    private final BrokerService brokerService = mock(BrokerService.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);

    private final URI transportConnectorUri = URI.create("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600");
    private final ReplicaSourceBroker source = new ReplicaSourceBroker(broker, transportConnectorUri);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final TransportConnector transportConnector = mock(TransportConnector.class);

    private final ActiveMQQueue testDestination = new ActiveMQQueue("TEST.QUEUE");

    @Before
    public void setUp() throws Exception {
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(broker.getAdminConnectionContext()).thenReturn(connectionContext);
        when(brokerService.getBroker()).thenReturn(source);
        when(brokerService.addConnector(transportConnectorUri)).thenReturn(transportConnector);
        when(connectionContext.isProducerFlowControl()).thenReturn(true);
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
        assertThat(replicationDestination.getPhysicalName()).isEqualTo(ReplicaSupport.REPLICATION_QUEUE_NAME);

        ActiveMQDestination precreatedDestination = destinations.get(1);
        assertThat(precreatedDestination).isEqualTo(testDestination);
    }

    @Test
    public void doesNotCreateDestinationEventsForNonReplicableDestinations() throws Exception {
        source.start();

        ActiveMQTopic advisoryTopic = new ActiveMQTopic(AdvisorySupport.ADVISORY_TOPIC_PREFIX + "TEST");
        source.addDestination(connectionContext, advisoryTopic, true);

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(2)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());


        List<ActiveMQDestination> destinations = destinationArgumentCaptor.getAllValues();

        ActiveMQDestination replicationDestination = destinations.get(0);
        assertThat(replicationDestination.getPhysicalName()).isEqualTo(ReplicaSupport.REPLICATION_QUEUE_NAME);

        ActiveMQDestination advisoryTopicDestination = destinations.get(1);
        assertThat(advisoryTopicDestination).isEqualTo(advisoryTopic);

        verify(broker, never()).send(any(), any());
    }

    @Test
    public void replicates_MESSAGE_SEND() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testDestination);

        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(connectionContext);

        source.send(producerExchange, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(2)).send(any(), messageArgumentCaptor.capture());

        final List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();

        ActiveMQMessage replicaMessage = values.get(0);
        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).isEqualTo(ReplicaSupport.REPLICATION_QUEUE_NAME);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.MESSAGE_SEND.name());
        assertThat(eventSerializer.deserializeMessageData(replicaMessage.getContent())).isEqualTo(message);

        ActiveMQMessage originalMessage = values.get(1);
        assertThat(originalMessage).isEqualTo(message);

        verifyConnectionContext(connectionContext);
    }

    private void verifyConnectionContext(ConnectionContext context) {
        verify(context).isProducerFlowControl();
        verify(context).setProducerFlowControl(false);
        verify(context).setProducerFlowControl(true);
    }
}
