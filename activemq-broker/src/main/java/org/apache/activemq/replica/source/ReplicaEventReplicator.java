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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.QueueBrowserSubscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.replica.util.DestinationExtractor;
import org.apache.activemq.replica.util.ReplicaEvent;
import org.apache.activemq.replica.util.ReplicaEventSerializer;
import org.apache.activemq.replica.util.ReplicaEventType;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ReplicaEventReplicator {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaEventReplicator.class);

    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {
    }; // used in destination map to indicate mirrored status

    private final AtomicBoolean initialized = new AtomicBoolean();
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    final DestinationMap destinationsToReplicate = new DestinationMap();


    private final Broker broker;
    private final ReplicationMessageProducer replicationMessageProducer;

    public ReplicaEventReplicator(Broker broker, ReplicationMessageProducer replicationMessageProducer) {
        this.broker = broker;
        this.replicationMessageProducer = replicationMessageProducer;
    }

    public void initialize() {
        initialized.compareAndSet(false, true);
    }

    public void deinitialize() {
        initialized.compareAndSet(true, false);
    }

    void ensureDestinationsAreReplicated() throws Exception {
        for (ActiveMQDestination d : broker.getDurableDestinations()) {
            if (!shouldReplicateDestination(d)) {
                continue;
            }
            ConnectionContext adminConnectionContext = broker.getAdminConnectionContext();
            replicateDestinationCreation(adminConnectionContext, d);

            if (!d.isTopic()) {
                continue;
            }

            List<DurableTopicSubscription> durableTopicSubscriptions = broker.getDestinations(d).stream().findFirst()
                    .map(DestinationExtractor::extractTopic)
                    .map(Topic::getDurableTopicSubs)
                    .stream()
                    .map(Map::values)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            for (DurableTopicSubscription sub : durableTopicSubscriptions) {
                replicateAddConsumer(adminConnectionContext, sub.getConsumerInfo(), sub.getContext().getClientId());
            }
        }
    }

    void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        if (!shouldReplicateDestination(destination)) {
            return;
        }
        if (destinationsToReplicate.chooseValue(destination) != null) {
            return;
        }

        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
            destinationsToReplicate.put(destination, IS_REPLICATED);
        } catch (Exception e) {
            logger.error("Failed to replicate creation of destination {}", destination.getPhysicalName(), e);
            throw e;
        }
    }

    private boolean shouldReplicateDestination(ActiveMQDestination destination) {
        boolean isReplicationQueue = ReplicaSupport.isReplicationDestination(destination);
        boolean isAdvisoryDestination = ReplicaSupport.isAdvisoryDestination(destination);
        boolean isTemporaryDestination = destination.isTemporary();
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination && !isTemporaryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        if (isTemporaryDestination) reason += "it is a temporary destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "" : "not ", destination, reason);
        return shouldReplicate;
    }

    public void replicateSend(ConnectionContext context, Message message, TransactionId transactionId) throws Exception {
        try {
            TransactionId originalTransactionId = message.getTransactionId();
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_SEND)
                            .setEventData(eventSerializer.serializeMessageData(message))
                            .setTransactionId(transactionId)
                            .setReplicationProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, message.getMessageId().toProducerKey())
                            .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY,
                                    message.getDestination().isQueue())
                            .setReplicationProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY,
                                    message.getDestination().toString())
                            .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY,
                                    originalTransactionId != null && originalTransactionId.isXATransaction())
            );
        } catch (Exception e) {
            logger.error("Failed to replicate message {} for destination {}", message.getMessageId(), message.getDestination().getPhysicalName(), e);
            throw e;
        }
    }

    public boolean needToReplicateSend(ConnectionContext connectionContext, Message message) {
        if (isReplicaContext(connectionContext)) {
            return false;
        }
        if (ReplicaSupport.isReplicationDestination(message.getDestination())) {
            return false;
        }
        if (message.getDestination().isTemporary()) {
            return false;
        }
        if (message.isAdvisory()) {
            return false;
        }
        if (!message.isPersistent()) {
            return false;
        }

        return true;
    }

    void replicateBeginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (!xid.isXATransaction()) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate begin of transaction [{}]", xid);
            throw e;
        }
    }

    void replicatePrepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (!xid.isXATransaction()) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction prepare [{}]", xid);
            throw e;
        }
    }

    void replicateForgetTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (!xid.isXATransaction()) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction forget [{}]", xid);
            throw e;
        }
    }

    void replicateRollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (!xid.isXATransaction()) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction rollback [{}]", xid);
            throw e;
        }
    }

    void replicateCommitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        if (!xid.isXATransaction()) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_COMMIT)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
                            .setReplicationProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY, onePhase)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate commit of transaction [{}]", xid);
            throw e;
        }
    }


    void replicateDestinationRemoval(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        if (!isReplicatedDestination(destination)) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.DESTINATION_DELETE)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
            destinationsToReplicate.remove(destination, IS_REPLICATED);
        } catch (Exception e) {
            logger.error("Failed to replicate remove of destination {}", destination.getPhysicalName(), e);
            throw e;
        }
    }

    void replicateAddConsumer(ConnectionContext context, ConsumerInfo consumerInfo, String clientId) throws Exception {
        if (!needToReplicateConsumer(consumerInfo)) {
            return;
        }
        if (ReplicaSupport.isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.ADD_DURABLE_CONSUMER)
                            .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
                            .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, clientId)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate adding {}", consumerInfo, e);
            throw e;
        }
    }

    void replicateRemoveConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        if (!needToReplicateConsumer(consumerInfo)) {
            return;
        }
        if (ReplicaSupport.isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.REMOVE_DURABLE_CONSUMER)
                            .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
                            .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, context.getClientId())
                            .setVersion(2)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate adding {}", consumerInfo, e);
            throw e;
        }
    }

    private boolean needToReplicateConsumer(ConsumerInfo consumerInfo) {
        return consumerInfo.getDestination().isTopic() &&
                consumerInfo.isDurable() &&
                !consumerInfo.isNetworkSubscription();
    }

    void replicateRemoveSubscription(ConnectionContext context, RemoveSubscriptionInfo subscriptionInfo) throws Exception {
        if (ReplicaSupport.isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.REMOVE_DURABLE_CONSUMER_SUBSCRIPTION)
                            .setEventData(eventSerializer.serializeReplicationData(subscriptionInfo))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate removing subscription {}", subscriptionInfo, e);
            throw e;
        }
    }

    boolean needToReplicateAck(ConnectionContext connectionContext, MessageAck ack, PrefetchSubscription subscription) {
        if (isReplicaContext(connectionContext)) {
            return false;
        }
        if (ReplicaSupport.isReplicationDestination(ack.getDestination())) {
            return false;
        }
        if (ack.getDestination().isTemporary()) {
            return false;
        }
        if (subscription instanceof QueueBrowserSubscription && !connectionContext.isNetworkConnection()) {
            return false;
        }

        return true;
    }

    void replicateAck(ConnectionContext connectionContext, MessageAck ack, PrefetchSubscription subscription,
            TransactionId transactionId, List<String> messageIdsToAck) throws Exception {
        try {
            TransactionId originalTransactionId = ack.getTransactionId();
            ActiveMQDestination destination = ack.getDestination();
            ReplicaEvent event = new ReplicaEvent()
                    .setEventType(ReplicaEventType.MESSAGE_ACK)
                    .setEventData(eventSerializer.serializeReplicationData(ack))
                    .setTransactionId(transactionId)
                    .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIdsToAck)
                    .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY,
                            destination.isQueue())
                    .setReplicationProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY,
                            destination.toString())
                    .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY,
                            originalTransactionId != null && originalTransactionId.isXATransaction());
            if (destination.isTopic() && subscription instanceof DurableTopicSubscription) {
                event.setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, connectionContext.getClientId());
                event.setReplicationProperty(ReplicaSupport.SUBSCRIPTION_NAME_PROPERTY,
                        ((DurableTopicSubscription) subscription).getSubscriptionKey().getSubscriptionName());
                event.setVersion(3);
            }

            enqueueReplicaEvent(connectionContext, event);
        } catch (Exception e) {
            logger.error("Failed to replicate ack messages [{} <-> {}] for consumer {}",
                    ack.getFirstMessageId(),
                    ack.getLastMessageId(),
                    ack.getConsumerId(), e);
            throw e;
        }
    }

     void replicateQueuePurged(ConnectionContext connectionContext, ActiveMQDestination destination) {
        try {
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.QUEUE_PURGED)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate queue purge {}", destination, e);
        }
    }

    void replicateMessageExpired(ConnectionContext context, MessageReference reference) {
        Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_EXPIRED)
                            .setEventData(eventSerializer.serializeReplicationData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate discard of {}", reference.getMessageId(), e);
        }
    }

    void enqueueHeartBeatEvent() {
        try {
            enqueueReplicaEvent(
                    broker.getAdminConnectionContext(),
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.HEART_BEAT)
                            .setEventData(eventSerializer.serializeReplicationData(null))
            );
        } catch (Exception e) {
            logger.error("Failed to send heart beat message", e);
        }
    }

    void enqueueFailOverEvent(ConnectionContext connectionContext, TransactionId tid) throws Exception {
        enqueueReplicaEvent(
                connectionContext,
                new ReplicaEvent()
                        .setEventType(ReplicaEventType.FAIL_OVER)
                        .setTransactionId(tid)
                        .setEventData(eventSerializer.serializeReplicationData(null))
        );
    }

    private void enqueueReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(connectionContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }
        replicationMessageProducer.enqueueIntermediateReplicaEvent(connectionContext, event);
    }

    private boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
    }
}
