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
package org.apache.activemq.replica.source;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.ReplicaPolicy;
import org.apache.activemq.replica.util.ReplicaEventType;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReplicaBatcherTest {

    ReplicaPolicy replicaPolicy = new ReplicaPolicy();

    @Test
    public void batchesSmallMessages() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        for (int i = 0; i < 1347; i++) {
            ActiveMQMessage message = new ActiveMQMessage();
            message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
            list.add(new DummyMessageReference(new MessageId("1:0:0:" + i), message, 1));
        }

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(replicaPolicy.getMaxBatchLength());
        for (int i = 0; i < replicaPolicy.getMaxBatchLength(); i++) {
            assertThat(batches.get(0).get(i).getMessageId().toString()).isEqualTo("1:0:0:" + i);
        }
        assertThat(batches.get(1).size()).isEqualTo(replicaPolicy.getMaxBatchLength());
        for (int i = 0; i < replicaPolicy.getMaxBatchLength(); i++) {
            assertThat(batches.get(1).get(i).getMessageId().toString()).isEqualTo("1:0:0:" + (i + replicaPolicy.getMaxBatchLength()));
        }
        assertThat(batches.get(2).size()).isEqualTo(347);
        for (int i = 0; i < 347; i++) {
            assertThat(batches.get(2).get(i).getMessageId().toString()).isEqualTo("1:0:0:" + (i + replicaPolicy.getMaxBatchLength() * 2));
        }
    }

    @Test
    public void batchesBigMessages() throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        List<MessageReference> list = new ArrayList<>();
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), message, replicaPolicy.getMaxBatchSize() + 1));
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), message, replicaPolicy.getMaxBatchSize() / 2 + 1));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), message, replicaPolicy.getMaxBatchSize() / 2));

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(1);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(1).size()).isEqualTo(1);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(2).size()).isEqualTo(1);
        assertThat(batches.get(2).get(0).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    @Test
    public void batchesAcksAfterSendsSameId() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:2");
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:1"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), activeMQMessage, 1));

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);
        assertThat(batches.size()).isEqualTo(2);
        assertThat(batches.get(0).size()).isEqualTo(2);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(0).get(1).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(1).size()).isEqualTo(1);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    @Test
    public void batchesAcksAfterSendsSameId2() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:1"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:1"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:4"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:2");
        list.add(new DummyMessageReference(new MessageId("1:0:0:5"), activeMQMessage, 1));

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(1);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(1).size()).isEqualTo(2);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(1).get(1).getMessageId().toString()).isEqualTo("1:0:0:3");
        assertThat(batches.get(2).size()).isEqualTo(2);
        assertThat(batches.get(2).get(0).getMessageId().toString()).isEqualTo("1:0:0:4");
        assertThat(batches.get(2).get(1).getMessageId().toString()).isEqualTo("1:0:0:5");
    }

    @Test
    public void batchesAcksAfterSendsDifferentIds() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:2");
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:4"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), activeMQMessage, 1));

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);
        assertThat(batches.size()).isEqualTo(1);
        assertThat(batches.get(0).size()).isEqualTo(3);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(0).get(1).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(0).get(2).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    @Test
    public void batchesFailOverMessageSeparately() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:5"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.FAIL_OVER.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:3");
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), activeMQMessage, 1));

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);
        assertThat(batches.size()).isEqualTo(2);
        assertThat(batches.get(0).size()).isEqualTo(2);
        assertThat(batches.get(1).size()).isEqualTo(1);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(0).get(1).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    @Test
    public void batchesTopicAckMessageSeparately() throws Exception {
        final List<MessageReference> list = new ArrayList<>();

        final String firstTopicSendMessageId = "1:0:0:1";
        final ActiveMQMessage firstTopicSend = new ActiveMQMessage();
        firstTopicSend.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "VirtualTopic.Orders");
        firstTopicSend.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        firstTopicSend.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        firstTopicSend.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        firstTopicSend.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, firstTopicSendMessageId);
        list.add(new DummyMessageReference(new MessageId(firstTopicSendMessageId), firstTopicSend, 1));

        final String secondTopicSendMessageId = "1:0:0:2";
        final ActiveMQMessage secondTopicSend = new ActiveMQMessage();
        secondTopicSend.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "VirtualTopic.Orders");
        secondTopicSend.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        secondTopicSend.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        secondTopicSend.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        secondTopicSend.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, secondTopicSendMessageId);
        list.add(new DummyMessageReference(new MessageId(secondTopicSendMessageId), secondTopicSend, 1));

        final String unrelatedTopicSendMessageId = "1:0:0:42"; // Not previously seen in the batch, should go to the same batch
        final ActiveMQMessage unrelatedConsumerQueueAck = new ActiveMQMessage();
        unrelatedConsumerQueueAck.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "Consumer.App1.VirtualTopic.Clients");
        unrelatedConsumerQueueAck.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        unrelatedConsumerQueueAck.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        unrelatedConsumerQueueAck.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        unrelatedConsumerQueueAck.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(unrelatedTopicSendMessageId));
        list.add(new DummyMessageReference(new MessageId(unrelatedTopicSendMessageId), unrelatedConsumerQueueAck, 1));

        final ActiveMQMessage firstConsumerQueueAck = new ActiveMQMessage(); // Previously seen in the batch, should go to a new batch
        firstConsumerQueueAck.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "Consumer.App1.VirtualTopic.Orders");
        firstConsumerQueueAck.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        firstConsumerQueueAck.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        firstConsumerQueueAck.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        firstConsumerQueueAck.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(firstTopicSendMessageId));
        list.add(new DummyMessageReference(new MessageId(firstTopicSendMessageId), firstConsumerQueueAck, 1));

        final String thirdTopicSendMessageId = "1:0:0:3";
        final ActiveMQMessage thirdTopicSend = new ActiveMQMessage();
        thirdTopicSend.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "VirtualTopic.Orders");
        thirdTopicSend.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        thirdTopicSend.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        thirdTopicSend.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        thirdTopicSend.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, thirdTopicSendMessageId);
        list.add(new DummyMessageReference(new MessageId(thirdTopicSendMessageId), thirdTopicSend, 1));

        List<List<MessageReference>> batches = new ReplicaBatcher(replicaPolicy).batches(list);

        assertThat(batches.size()).isEqualTo(2);

        assertThat(batches.get(0).size()).isEqualTo(3);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo(firstTopicSendMessageId);
        assertThat(batches.get(0).get(1).getMessageId().toString()).isEqualTo(secondTopicSendMessageId);
        assertThat(batches.get(0).get(2).getMessageId().toString()).isEqualTo(unrelatedTopicSendMessageId);

        assertThat(batches.get(1).size()).isEqualTo(2);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo(firstTopicSendMessageId);
        assertThat(batches.get(1).get(1).getMessageId().toString()).isEqualTo(thirdTopicSendMessageId);
    }

    @Test
    public void topicSendTriggeringBatchSplitBySize_ackLandsInSeparateBatch() throws Exception {
        final ReplicaPolicy policy = new ReplicaPolicy();
        policy.setMaxBatchSize(10);

        final List<MessageReference> list = new ArrayList<>();

        // Topic SEND "msg-1" — takes up most of the batch size budget
        final ActiveMQMessage send1 = new ActiveMQMessage();
        send1.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "topic://VirtualTopic.Orders");
        send1.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        send1.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        send1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        send1.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "msg-1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), send1, 8));

        // Topic SEND "msg-2" — exceeds maxBatchSize, triggers split
        final ActiveMQMessage send2 = new ActiveMQMessage();
        send2.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "topic://VirtualTopic.Orders");
        send2.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        send2.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        send2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        send2.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "msg-2");
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), send2, 8));

        // ACK for "msg-2" from Virtual Topic consumer queue, should trigger a new batch
        final ActiveMQMessage ack = new ActiveMQMessage();
        ack.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "queue://Consumer.App1.VirtualTopic.Orders");
        ack.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, "false");
        ack.setStringProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY, "false");
        ack.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        ack.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("msg-2"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), ack, 1));

        final List<List<MessageReference>> batches = new ReplicaBatcher(policy).batches(list);

        assertThat(batches).hasSize(3);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(2).get(0).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    private static class DummyMessageReference implements MessageReference {

        private final MessageId messageId;
        private Message message;
        private final int size;

        DummyMessageReference(MessageId messageId, Message message, int size) {
            this.messageId = messageId;
            this.message = message;
            this.size = size;
        }

        @Override
        public MessageId getMessageId() {
            return messageId;
        }

        @Override
        public Message getMessageHardRef() {
            return null;
        }

        @Override
        public Message getMessage() {
            return message;
        }

        @Override
        public boolean isPersistent() {
            return false;
        }

        @Override
        public Message.MessageDestination getRegionDestination() {
            return null;
        }

        @Override
        public int getRedeliveryCounter() {
            return 0;
        }

        @Override
        public void incrementRedeliveryCounter() {

        }

        @Override
        public int getReferenceCount() {
            return 0;
        }

        @Override
        public int incrementReferenceCount() {
            return 0;
        }

        @Override
        public int decrementReferenceCount() {
            return 0;
        }

        @Override
        public ConsumerId getTargetConsumerId() {
            return null;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public long getExpiration() {
            return 0;
        }

        @Override
        public String getGroupID() {
            return null;
        }

        @Override
        public int getGroupSequence() {
            return 0;
        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public boolean isDropped() {
            return false;
        }

        @Override
        public boolean isAdvisory() {
            return false;
        }

        @Override
        public boolean canProcessAsExpired() {
            return false;
        }
    }
}
