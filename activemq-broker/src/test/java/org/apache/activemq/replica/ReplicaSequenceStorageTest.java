/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ReplicaSequenceStorageTest {

    private final static String SEQUENCE_NAME = "testSeq";
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final Broker broker = mock(Broker.class);
    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final Queue sequenceQueue = mock(Queue.class);
    private final ActiveMQQueue sequenceQueueDestination = new ActiveMQQueue(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    private final PrefetchSubscription subscription = mock(PrefetchSubscription.class);
    private final ReplicaInternalMessageProducer replicaProducer = mock(ReplicaInternalMessageProducer.class);


    private ReplicaSequenceStorage replicaSequenceStorage;

    @Before
    public void setUp() throws Exception {
        when(broker.getDestinations(any())).thenReturn(Set.of(sequenceQueue));
        ConnectionContext adminConnectionContext = mock(ConnectionContext.class);
        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);
        when(queueProvider.getSequenceQueue()).thenReturn(sequenceQueueDestination);
        when(sequenceQueue.getAllMessageIds()).thenReturn(new ArrayList<>());

        this.replicaSequenceStorage = new ReplicaSequenceStorage(broker, connectionContext, queueProvider, replicaProducer, SEQUENCE_NAME);
    }

    @Test
    public void shouldInitializeWhenNoMessagesExist() throws Exception {
        when(sequenceQueue.getAllMessageIds()).thenReturn(new ArrayList<>());

        replicaSequenceStorage.initialize();
        verify(sequenceQueue, never()).removeMessage(any());
    }

    @Test
    public void shouldInitializeWhenMoreThanOneExist() throws Exception {
        ActiveMQTextMessage message1 = new ActiveMQTextMessage();
        message1.setMessageId(new MessageId("1:0:0:1"));
        message1.setText("1");
        message1.setStringProperty(ReplicaSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);
        ActiveMQTextMessage message2 = new ActiveMQTextMessage();
        message2.setMessageId(new MessageId("1:0:0:2"));
        message2.setText("2");
        message2.setStringProperty(ReplicaSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        QueueMessageReference messageReference1 = mock(QueueMessageReference.class);
        when(messageReference1.getMessage()).thenReturn(message1);
        QueueMessageReference messageReference2 = mock(QueueMessageReference.class);
        when(messageReference2.getMessage()).thenReturn(message2);

        when(sequenceQueue.getMessage(messageId1.toString())).thenReturn(messageReference1);
        when(sequenceQueue.getMessage(messageId2.toString())).thenReturn(messageReference2);

        when(sequenceQueue.getAllMessageIds()).thenReturn(List.of(messageId1, messageId2));

        replicaSequenceStorage.initialize();
        verify(sequenceQueue, times(1)).removeMessage(eq(message1.getMessageId().toString()));
    }

    @Test
    public void initializeWhenMoreThanOneExist() throws Exception {
        MessageId messageId1 = new MessageId("1:0:0:1");
        ActiveMQTextMessage message1 = new ActiveMQTextMessage();
        message1.setMessageId(messageId1);
        message1.setText("1");
        message1.setStringProperty(ReplicaSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);
        MessageId messageId2 = new MessageId("1:0:0:2");
        ActiveMQTextMessage message2 = new ActiveMQTextMessage();
        message2.setMessageId(messageId2);
        message2.setText("2");
        message2.setStringProperty(ReplicaSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);
        QueueMessageReference messageReference1 = mock(QueueMessageReference.class);
        when(messageReference1.getMessage()).thenReturn(message1);
        QueueMessageReference messageReference2 = mock(QueueMessageReference.class);
        when(messageReference2.getMessage()).thenReturn(message2);
        when(sequenceQueue.getMessage(messageId1.toString())).thenReturn(messageReference1);
        when(sequenceQueue.getMessage(messageId2.toString())).thenReturn(messageReference2);
        when(sequenceQueue.getAllMessageIds()).thenReturn(List.of(messageId1, messageId2));
        String savedSequence = replicaSequenceStorage.initialize();

        assertThat(savedSequence).isEqualTo(message1.getText());
    }

    @Test
    public void shouldEnqueueMessage() throws Exception {
        String messageToEnqueue = "THIS IS A MESSAGE";
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ArgumentCaptor<ActiveMQTextMessage> activeMQTextMessageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQTextMessage.class);
        when(subscription.getDispatched()).thenReturn(new ArrayList<>());
        when(broker.addConsumer(any(ConnectionContext.class), any(ConsumerInfo.class))).thenReturn(subscription);
        replicaSequenceStorage.initialize();

        replicaSequenceStorage.enqueue(transactionId, messageToEnqueue);

        verify(replicaProducer, times(1)).sendIgnoringFlowControl(any(), activeMQTextMessageArgumentCaptor.capture());
        assertThat(activeMQTextMessageArgumentCaptor.getValue().getText()).isEqualTo(messageToEnqueue);
        assertThat(activeMQTextMessageArgumentCaptor.getValue().getTransactionId()).isEqualTo(transactionId);
        assertThat(activeMQTextMessageArgumentCaptor.getValue().getDestination()).isEqualTo(sequenceQueueDestination);
        assertThat(activeMQTextMessageArgumentCaptor.getValue().isPersistent()).isTrue();
        assertThat(activeMQTextMessageArgumentCaptor.getValue().isResponseRequired()).isFalse();
        reset(broker);
        reset(subscription);
    }

    @Test
    public void shouldAcknowledgeAllMessagesWhenEnqueue() throws Exception {
        ActiveMQTextMessage message1 = new ActiveMQTextMessage();
        message1.setMessageId(new MessageId("1:0:0:1"));
        message1.setText("1");
        message1.setStringProperty(ReplicaSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);
        ActiveMQTextMessage message2 = new ActiveMQTextMessage();
        message2.setMessageId(new MessageId("1:0:0:3"));
        message2.setText("3");
        message2.setStringProperty(ReplicaSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);

        QueueMessageReference messageReference1 = mock(QueueMessageReference.class);
        when(messageReference1.getMessage()).thenReturn(message1);
        when(messageReference1.getMessageId()).thenReturn(message1.getMessageId());
        QueueMessageReference messageReference2 = mock(QueueMessageReference.class);
        when(messageReference2.getMessage()).thenReturn(message2);
        when(messageReference2.getMessageId()).thenReturn(message2.getMessageId());

        when(subscription.getDispatched()).thenReturn(List.of(messageReference1, messageReference2));
        when(broker.addConsumer(any(ConnectionContext.class), any(ConsumerInfo.class))).thenReturn(subscription);
        replicaSequenceStorage.initialize();

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);

        String messageToEnqueue = "THIS IS A MESSAGE";
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);

        replicaSequenceStorage.enqueue(transactionId, messageToEnqueue);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());
        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getFirstMessageId()).isEqualTo(message1.getMessageId());
        assertThat(value.getLastMessageId()).isEqualTo(message2.getMessageId());
        assertThat(value.getDestination()).isEqualTo(sequenceQueueDestination);
        assertThat(value.getMessageCount()).isEqualTo(2);
        assertThat(value.getAckType()).isEqualTo(MessageAck.STANDARD_ACK_TYPE);

    }
}
