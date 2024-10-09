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
import org.apache.activemq.command.RemoveSubscriptionInfo;
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

    private final Map<DurableTopicSubscription, MessageId> lastUnackedMessages;
    private final List<DurableTopicSubscription> durableTopicSubscriptions;

    private ReplicaTopicSynchronizer(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage, ConnectionContext connectionContext,
            ActiveMQTopic destination, MessageId restoreMessageId, List<DurableTopicSubscription> durableTopicSubscriptions) throws Exception {
        super(broker, replicator, storage, connectionContext, destination, restoreMessageId);
        this.durableTopicSubscriptions = durableTopicSubscriptions;
        lastUnackedMessages = loadUnackedMessages();
    }

    static void resynchronize(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage,
            ConnectionContext connectionContext, ActiveMQTopic destination, MessageId restoreMessageId) throws Exception {
        List<DurableTopicSubscription> durableTopicSubscriptions = broker.getDestinations(destination).stream().findFirst()
                .map(DestinationExtractor::extractTopic)
                .map(Topic::getDurableTopicSubs)
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        if (durableTopicSubscriptions.isEmpty()) {
            return;
        }

        new ReplicaTopicSynchronizer(broker, replicator, storage, connectionContext, destination, restoreMessageId, durableTopicSubscriptions).resynchronize();
    }

    @Override
    void preRecover() throws Exception {
        for (DurableTopicSubscription sub : durableTopicSubscriptions) {
            RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
            info.setSubcriptionName(sub.getConsumerInfo().getSubscriptionName());
            info.setConnectionId(sub.getContext().getConnectionId());
            info.setClientId(sub.getContext().getClientId());
            replicator.replicateRemoveSubscription(connectionContext, info);
        }

        super.preRecover();

        for (DurableTopicSubscription sub : durableTopicSubscriptions) {
            replicator.replicateAddConsumer(connectionContext, sub.getConsumerInfo(), sub.getContext().getClientId());
        }
    }

    @Override
    public void processMessage(Message message, TransactionId tid) throws Exception {
        replicator.replicateSend(connectionContext, message, tid);
        replicateAcksIfNeeded(tid, message.getMessageId());
    }

    private Map<DurableTopicSubscription, MessageId> loadUnackedMessages() throws Exception {
        Map<DurableTopicSubscription, MessageId> result = new HashMap<>();
        for (DurableTopicSubscription sub : durableTopicSubscriptions) {
            result.put(sub, getNextMessageId(sub));
        }
        return result;
    }

    private void replicateAcksIfNeeded(TransactionId tid, MessageId currentMessageId) throws Exception {
        for (Map.Entry<DurableTopicSubscription, MessageId> entry : lastUnackedMessages.entrySet()) {
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

    private void replicateAck(TransactionId tid, MessageId messageId, DurableTopicSubscription sub) throws Exception {
        MessageAck ack = new MessageAck();
        ack.setConsumerId(sub.getConsumerInfo().getConsumerId());
        ack.setDestination(destination);
        ack.setMessageID(messageId);
        ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);

        replicator.replicateAck(connectionContext, ack, sub, tid, List.of(messageId.toString()), sub.getContext().getClientId());
    }

    private MessageId getNextMessageId(DurableTopicSubscription sub) throws Exception {
        SubscriptionKey subscriptionKey = sub.getSubscriptionKey();
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
