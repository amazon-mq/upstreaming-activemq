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
package org.apache.activemq.replica.source.resync;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.storage.ReplicaResynchronizationStorage;
import org.apache.activemq.replica.util.DestinationExtractor;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.SubscriptionKey;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ReplicaTopicSynchronizer extends ReplicaDestinationSynchronizer {

    private final List<SubscriptionKey> subscriptionKeys;
    private final Map<SubscriptionKey, MessageId> lastUnackedMessages;

    private ReplicaTopicSynchronizer(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage, ConnectionContext connectionContext,
            ActiveMQTopic destination, MessageId restoreMessageId, List<SubscriptionKey> subscriptionKeys) throws Exception {
        super(broker, replicator, storage, connectionContext, destination, restoreMessageId);
        this.subscriptionKeys = subscriptionKeys;
        lastUnackedMessages = loadUnackedMessages();
    }

    static void resynchronize(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage,
            ConnectionContext connectionContext, ActiveMQTopic destination, MessageId restoreMessageId) throws Exception {
        List<SubscriptionKey> subscriptionKeys = broker.getDestinations(destination).stream().findFirst()
                .map(DestinationExtractor::extractTopic)
                .map(Topic::getDurableTopicSubs)
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .map(DurableTopicSubscription::getSubscriptionKey)
                .collect(Collectors.toList());
        if (subscriptionKeys.isEmpty()) {
            return;
        }

        new ReplicaTopicSynchronizer(broker, replicator, storage, connectionContext, destination, restoreMessageId, subscriptionKeys).resynchronize();
    }

    @Override
    void preRecover() throws Exception {
        for (SubscriptionKey subscriptionKey : subscriptionKeys) {
            ConsumerInfo consumerInfo = new ConsumerInfo();
            consumerInfo.setDestination(destination);
            consumerInfo.setSubscriptionName(subscriptionKey.getSubscriptionName());
            replicator.replicateAddConsumer(connectionContext, consumerInfo, subscriptionKey.getClientId());
        }
    }

    @Override
    public void processMessage(Message message, TransactionId tid) throws Exception {
        replicator.replicateSend(connectionContext, message, tid);
        replicateAcksIfNeeded(tid, message.getMessageId());
    }

    private Map<SubscriptionKey, MessageId> loadUnackedMessages() throws Exception {
        Map<SubscriptionKey, MessageId> result = new HashMap<>();
        for (SubscriptionKey subscriptionKey : subscriptionKeys) {
            result.put(subscriptionKey, getNextMessageId(subscriptionKey));
        }
        return result;
    }

    private void replicateAcksIfNeeded(TransactionId tid, MessageId currentMessageId) throws Exception {
        for (Map.Entry<SubscriptionKey, MessageId> entry : lastUnackedMessages.entrySet()) {
            if (entry.getValue() == null) {
                replicateAck(tid, currentMessageId, entry.getKey());
                continue;
            }
            if (entry.getValue().equals(currentMessageId)) {
                entry.setValue(getNextMessageId(entry.getKey()));
                continue;
            }

            replicateAck(tid, currentMessageId, entry.getKey());
        }
    }

    private void replicateAck(TransactionId tid, MessageId messageId, SubscriptionKey subscriptionKey) throws Exception {
        MessageAck ack = new MessageAck();
        ack.setDestination(destination);
        ack.setMessageID(messageId);
        ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);

        replicator.replicateAck(connectionContext, ack, subscriptionKey.getClientId(), subscriptionKey.getSubscriptionName(),
                tid, List.of(messageId.toString()));
    }

    private MessageId getNextMessageId(SubscriptionKey subscriptionKey) throws Exception {
        SingleMessageRecoveryListener listener = new SingleMessageRecoveryListener();
        ((TopicMessageStore) messageStore).recoverNextMessages(subscriptionKey.getClientId(),
                subscriptionKey.getSubscriptionName(), 1, listener);
        return listener.id.get();
    }

    private static class SingleMessageRecoveryListener implements MessageRecoveryListener {
        private final AtomicReference<MessageId> id = new AtomicReference<>();

        public SingleMessageRecoveryListener() {
        }

        @Override
        public boolean recoverMessage(Message message) throws Exception {
            id.set(message.getMessageId());
            return true;
        }

        @Override
        public boolean recoverMessageReference(MessageId ref) throws Exception {
            return false;
        }

        @Override
        public boolean hasSpace() {
            return false;
        }

        @Override
        public boolean isDuplicate(MessageId ref) {
            return false;
        }
    }
}
