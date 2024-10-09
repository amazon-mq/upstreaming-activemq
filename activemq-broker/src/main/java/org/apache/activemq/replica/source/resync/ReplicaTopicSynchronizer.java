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
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.DestinationExtractor;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ReplicaTopicSynchronizer {

    private final Logger logger = LoggerFactory.getLogger(ReplicaTopicSynchronizer.class);

    private final Broker broker;
    private final ReplicaEventReplicator replicator;
    private final ActiveMQTopic destination;
    private final ConnectionContext connectionContext;
    private final List<SubscriptionKey> subscriptionKeys;
    private final AtomicInteger processedCount = new AtomicInteger();
    private final TopicMessageStore messageStore;
    private final Map<SubscriptionKey, MessageId> lastUnackedMessages;

    private ReplicaTopicSynchronizer(Broker broker, ReplicaEventReplicator replicator, ConnectionContext connectionContext,
            ActiveMQTopic destination, List<SubscriptionKey> subscriptionKeys) throws Exception {
        this.broker = broker;
        this.replicator = replicator;
        this.destination = destination;
        this.connectionContext = connectionContext;
        this.subscriptionKeys = subscriptionKeys;
        Topic topic = broker.getDestinations(destination).stream().findFirst().map(DestinationExtractor::extractTopic).orElseThrow();
        messageStore = (TopicMessageStore) topic.getMessageStore();
        lastUnackedMessages = loadUnackedMessages(messageStore, subscriptionKeys);
    }

    static void resynchronize(Broker broker, ReplicaEventReplicator replicator, ConnectionContext connectionContext, ActiveMQTopic destination) throws Exception {
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

        new ReplicaTopicSynchronizer(broker, replicator, connectionContext, destination, subscriptionKeys).resynchronize();
    }

    private void resynchronize() throws Exception {
        logger.info("Resyncronizing: " + destination);

        replicator.replicateDestinationRemoval(connectionContext, destination);
        replicator.replicateDestinationCreation(connectionContext, destination);

        for (SubscriptionKey subscriptionKey : subscriptionKeys) {
            ConsumerInfo consumerInfo = new ConsumerInfo();
            consumerInfo.setDestination(destination);
            consumerInfo.setSubscriptionName(subscriptionKey.getSubscriptionName());
            replicator.replicateAddConsumer(connectionContext, consumerInfo, subscriptionKey.getClientId());
        }

        messageStore.recover(new TopicRecoveryListener());
    }

    private Map<SubscriptionKey, MessageId> loadUnackedMessages(TopicMessageStore messageStore,
            List<SubscriptionKey> subscriptionKeys) throws Exception {
        Map<SubscriptionKey, MessageId> result = new HashMap<>();
        for (SubscriptionKey subscriptionKey : subscriptionKeys) {
            result.put(subscriptionKey, getNextMessageId(messageStore, subscriptionKey));
        }
        return result;
    }

    private void replicateAcksIfNeeded(TransactionId tid, MessageId currentMessageId) throws Exception {
        for (Map.Entry<SubscriptionKey, MessageId> entry : lastUnackedMessages.entrySet()) {
            if (entry.getValue() == null) {
                replicateAck(connectionContext, tid, currentMessageId, entry.getKey());
                continue;
            }
            if (entry.getValue().equals(currentMessageId)) {
                entry.setValue(getNextMessageId(messageStore, entry.getKey()));
                continue;
            }

            replicateAck(connectionContext, tid, currentMessageId, entry.getKey());
        }
    }

    private void replicateAck(ConnectionContext context, TransactionId tid, MessageId messageId, SubscriptionKey subscriptionKey) throws Exception {
        MessageAck ack = new MessageAck();
        ack.setDestination(destination);
        ack.setMessageID(messageId);
        ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);

        replicator.replicateAck(context, ack, subscriptionKey.getClientId(), subscriptionKey.getSubscriptionName(),
                tid, List.of(messageId.toString()));
    }

    private MessageId getNextMessageId(TopicMessageStore messageStore, SubscriptionKey subscriptionKey) throws Exception {
        AtomicReference<MessageId> id = new AtomicReference<>();
        messageStore.recoverNextMessages(subscriptionKey.getClientId(), subscriptionKey.getSubscriptionName(), 1, new ReplicaMessageRecoveryListener() {
            @Override
            public boolean recoverMessage(Message message) throws Exception {
                id.set(message.getMessageId());
                return true;
            }

            @Override
            public boolean hasSpace() {
                return false;
            }
        });
        return id.get();
    }

    private class TopicRecoveryListener extends ReplicaMessageRecoveryListener {

        @Override
        public boolean recoverMessage(Message message) throws Exception {
            int val = processedCount.incrementAndGet();
            if (val % 10000 == 0) {
                logger.info("Progress: " + val);
            }

            LocalTransactionId tid = new LocalTransactionId(
                    new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

            broker.beginTransaction(connectionContext, tid);
            try {
                replicator.replicateSend(connectionContext, message, tid);
                replicateAcksIfNeeded(tid, message.getMessageId());

                broker.commitTransaction(connectionContext, tid, true);
            } catch (Exception e) {
                broker.rollbackTransaction(connectionContext, tid);
                logger.error("Failed", e);
                throw e;
            }
            return true;
        }
    }
}
