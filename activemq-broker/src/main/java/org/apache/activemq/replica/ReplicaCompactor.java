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
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ReplicaCompactor {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaCompactor.class);
    private static final String CONSUMER_SELECTOR = String.format("%s LIKE '%s'", ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK);
    public static final int MAXIMUM_MESSAGES = 1_000;

    private final Broker broker;
    private final ConnectionContext connectionContext;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final PrefetchSubscription subscription;
    private final AtomicLong tpsCounter;

    private final Queue intermediateQueue;

    public ReplicaCompactor(Broker broker, ConnectionContext connectionContext, ReplicaReplicationQueueSupplier queueProvider, PrefetchSubscription subscription, AtomicLong tpsCounter) {
        this.broker = broker;
        this.connectionContext = connectionContext;
        this.queueProvider = queueProvider;
        this.subscription = subscription;
        this.tpsCounter = tpsCounter;

        intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
    }

    List<MessageReference> compactAndFilter(List<MessageReference> list, boolean withAdditionalMessages) throws Exception {
        List<DeliveredMessageReference> toProcess = list.stream()
                .map(DeliveredMessageReference::new)
                .collect(Collectors.toList());

        int prefetchSize = subscription.getPrefetchSize();
        try {
            if (withAdditionalMessages) {
                subscription.setPrefetchSize(0);
                toProcess.addAll(getAdditionalMessages());
            }

            List<DeliveredMessageReference> processed = compactAndFilter0(toProcess);

            Set<MessageId> messageIds = list.stream().map(MessageReference::getMessageId).collect(Collectors.toSet());

            return processed.stream()
                    .map(dmr -> dmr.messageReference)
                    .filter(mr -> messageIds.contains(mr.getMessageId()))
                    .collect(Collectors.toList());
        } finally {
            subscription.setPrefetchSize(prefetchSize);
        }
    }

    private List<DeliveredMessageReference> getAdditionalMessages() throws Exception {
        List<DeliveredMessageReference> result = new ArrayList<>();
        List<QueueMessageReference> additionalMessages = intermediateQueue.getMatchingMessages(connectionContext, CONSUMER_SELECTOR, MAXIMUM_MESSAGES);
        if (additionalMessages.isEmpty()) {
            return result;
        }

        String selector = String.format("%s IN %s", ReplicaSupport.MESSAGE_ID_PROPERTY, getAckedMessageIds(additionalMessages));
        additionalMessages.addAll(intermediateQueue.getMatchingMessages(connectionContext, selector, MAXIMUM_MESSAGES));

        Set<MessageId> dispatchedMessageIds = subscription.getDispatched().stream()
                .map(MessageReference::getMessageId)
                .collect(Collectors.toSet());

        for (MessageReference messageReference : additionalMessages) {
            if (!dispatchedMessageIds.contains(messageReference.getMessageId())) {
                result.add(new DeliveredMessageReference(messageReference, false));
            }
        }

        return result;
    }

    private List<DeliveredMessageReference> compactAndFilter0(List<DeliveredMessageReference> list) throws Exception {
        List<DeliveredMessageReference> result = new ArrayList<>(list);

        List<Destination> destinations = combineByDestination(list);

        List<DeliveredMessageId> toDelete = compact(destinations);

        if (toDelete.isEmpty()) {
            return result;
        }

        acknowledge(toDelete);

        List<MessageId> messageIds = toDelete.stream().map(dmid -> dmid.messageId).collect(Collectors.toList());
        result.removeIf(reference -> messageIds.contains(reference.messageReference.getMessageId()));

        tpsCounter.addAndGet(toDelete.size());

        return result;
    }

    private void acknowledge(List<DeliveredMessageId> list) throws Exception {
        TransactionId transactionId = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
            broker.beginTransaction(connectionContext, transactionId);

            ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
            consumerExchange.setConnectionContext(connectionContext);

            for (DeliveredMessageId deliveredMessageId : list) {
                if (!deliveredMessageId.delivered) {
                    messageDispatch(deliveredMessageId.messageId);
                }

                MessageAck messageAck = new MessageAck();
                messageAck.setMessageID(deliveredMessageId.messageId);
                messageAck.setMessageCount(1);
                messageAck.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
                messageAck.setDestination(queueProvider.getIntermediateQueue());

                consumerExchange.setSubscription(subscription);

                broker.acknowledge(consumerExchange, messageAck);
            }

            broker.commitTransaction(connectionContext, transactionId, true);
        }
    }

    private List<Destination> combineByDestination(List<DeliveredMessageReference> list) throws Exception {
        Map<String, Destination> result = new HashMap<>();
        for (DeliveredMessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.messageReference.getMessage();

            ReplicaEventType eventType =
                    ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
            if (eventType != ReplicaEventType.MESSAGE_SEND && eventType != ReplicaEventType.MESSAGE_ACK) {
                continue;
            }

            if (!message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY)
                    || message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY)) {
                continue;
            }

            Destination destination =
                    result.computeIfAbsent(message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY),
                            k -> new Destination());

            if (eventType == ReplicaEventType.MESSAGE_SEND) {
                destination.sendMap.put(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY),
                        new DeliveredMessageId(message.getMessageId(), reference.delivered));
            }
            if (eventType == ReplicaEventType.MESSAGE_ACK) {
                List<String> messageIds = getAckMessageIds(message);
                destination.acks.add(new Ack(messageIds, message, reference.delivered));
            }
        }

        return new ArrayList<>(result.values());
    }

    private List<DeliveredMessageId> compact(List<Destination> destinations) throws IOException {
        List<DeliveredMessageId> result = new ArrayList<>();
        for (Destination destination : destinations) {
            for (Ack ack : destination.acks) {
                List<String> sends = new ArrayList<>();
                for (String id : ack.messageIdsToAck) {
                    if (destination.sendMap.containsKey(id)) {
                        sends.add(id);
                        result.add(destination.sendMap.get(id));
                    }
                }
                if (sends.size() == 0) {
                    continue;
                }

                if (ack.messageIdsToAck.size() == sends.size() && new HashSet<>(ack.messageIdsToAck).containsAll(sends)) {
                    result.add(ack);
                } else {
                    updateMessage(ack.message, ack.messageIdsToAck, sends);
                }
            }
        }

        return result;
    }

    private void updateMessage(ActiveMQMessage message, List<String> messageIdsToAck, List<String> sends) throws IOException {
        message.setProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY, messageIdsToAck);
        ArrayList<String> newList = new ArrayList<>(messageIdsToAck);
        newList.removeAll(sends);
        message.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, newList);

        synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
            intermediateQueue.getMessageStore().updateMessage(message);
        }
    }

    private String getAckedMessageIds(List<QueueMessageReference> ackMessages) throws IOException {
        List<String> messageIds = new ArrayList<>();
        for (QueueMessageReference messageReference : ackMessages) {
            ActiveMQMessage message = (ActiveMQMessage) messageReference.getMessage();

            messageIds.addAll(getAckMessageIds(message));
        }

        return messageIds.stream().collect(Collectors.joining("','", "('", "')"));
    }

    private void messageDispatch(MessageId messageId) throws Exception {
        MessageDispatchNotification mdn = new MessageDispatchNotification();
        mdn.setConsumerId(subscription.getConsumerInfo().getConsumerId());
        mdn.setDestination(queueProvider.getIntermediateQueue());
        mdn.setMessageId(messageId);
        broker.processDispatchNotification(mdn);
    }

    @SuppressWarnings("unchecked")
    private static List<String> getAckMessageIds(ActiveMQMessage message) throws IOException {
        return (List<String>)
                Optional.ofNullable(message.getProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY))
                        .orElse(message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));
    }

    private static class DeliveredMessageReference {
        final MessageReference messageReference;
        final boolean delivered;

        public DeliveredMessageReference(MessageReference messageReference) {
            this(messageReference, true);
        }

        public DeliveredMessageReference(MessageReference messageReference, boolean delivered) {
            this.messageReference = messageReference;
            this.delivered = delivered;
        }
    }

    private static class Destination {
        final Map<String, DeliveredMessageId> sendMap = new LinkedHashMap<>();
        final List<Ack> acks = new ArrayList<>();
    }

    private static class Ack extends DeliveredMessageId {
        final List<String> messageIdsToAck;
        final ActiveMQMessage message;

        public Ack(List<String> messageIdsToAck, ActiveMQMessage message, boolean needsDelivery) {
            super(message.getMessageId(), needsDelivery);
            this.messageIdsToAck = messageIdsToAck;
            this.message = message;
        }
    }

    private static class DeliveredMessageId {
        final MessageId messageId;
        final boolean delivered;

        public DeliveredMessageId(MessageId messageId, boolean delivered) {
            this.messageId = messageId;
            this.delivered = delivered;
        }
    }
}
