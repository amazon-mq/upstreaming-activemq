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
package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.util.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.util.ReplicaSupport;
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
        when(broker.addConsumer(any(), any())).thenReturn(subscription);
        when(queueProvider.getSequenceQueue()).thenReturn(sequenceQueueDestination);

        this.replicaSequenceStorage = new ReplicaSequenceStorage(broker, queueProvider, replicaProducer, SEQUENCE_NAME);
    }

    @Test
    public void shouldInitializeWhenNoMessagesExist() throws Exception {
        when(subscription.getDispatched()).thenReturn(new ArrayList<>()).thenReturn(new ArrayList<>());

        String initialize = replicaSequenceStorage.initialize(connectionContext);
        assertThat(initialize).isNull();
        verify(sequenceQueue, never()).removeMessage(any());
    }

    @Test
    public void shouldInitializeWhenMoreThanOneExist() throws Exception {
        ActiveMQTextMessage message1 = new ActiveMQTextMessage();
        message1.setMessageId(new MessageId("1:0:0:1"));
        message1.setText("1");
        message1.setStringProperty(ReplicaBaseSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);
        ActiveMQTextMessage message2 = new ActiveMQTextMessage();
        message2.setMessageId(new MessageId("1:0:0:2"));
        message2.setText("2");
        message2.setStringProperty(ReplicaBaseSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);

        when(subscription.getDispatched())
                .thenReturn(List.of(new IndirectMessageReference(message1), new IndirectMessageReference(message2)));

        String initialize = replicaSequenceStorage.initialize(connectionContext);
        assertThat(initialize).isEqualTo(message2.getText());

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());
        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getFirstMessageId()).isEqualTo(message1.getMessageId());
        assertThat(value.getLastMessageId()).isEqualTo(message1.getMessageId());
        assertThat(value.getDestination()).isEqualTo(sequenceQueueDestination);
        assertThat(value.getMessageCount()).isEqualTo(1);
        assertThat(value.getAckType()).isEqualTo(MessageAck.STANDARD_ACK_TYPE);
    }

    @Test
    public void shouldEnqueueMessage() throws Exception {
        String messageToEnqueue = "THIS IS A MESSAGE";
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ArgumentCaptor<ActiveMQTextMessage> activeMQTextMessageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQTextMessage.class);
        when(subscription.getDispatched()).thenReturn(new ArrayList<>());
        replicaSequenceStorage.initialize(connectionContext);

        replicaSequenceStorage.enqueue(connectionContext, transactionId, messageToEnqueue);

        verify(replicaProducer, times(1)).sendForcingFlowControl(any(), activeMQTextMessageArgumentCaptor.capture());
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

        when(subscription.getDispatched()).thenReturn(List.of(), List.of(messageReference1, messageReference2));
        replicaSequenceStorage.initialize(connectionContext);

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);

        String messageToEnqueue = "THIS IS A MESSAGE";
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);

        replicaSequenceStorage.enqueue(connectionContext, transactionId, messageToEnqueue);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());
        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getFirstMessageId()).isEqualTo(message1.getMessageId());
        assertThat(value.getLastMessageId()).isEqualTo(message2.getMessageId());
        assertThat(value.getDestination()).isEqualTo(sequenceQueueDestination);
        assertThat(value.getMessageCount()).isEqualTo(2);
        assertThat(value.getAckType()).isEqualTo(MessageAck.STANDARD_ACK_TYPE);

    }
}
