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
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.MutativeRoleBroker;
import org.apache.activemq.replica.ReplicaPolicy;
import org.apache.activemq.replica.ReplicaReplicationDestinationSupplier;
import org.apache.activemq.replica.ReplicaRoleManagement;
import org.apache.activemq.replica.source.resync.ReplicaResynchronizer;
import org.apache.activemq.replica.util.ReplicaEventType;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.activemq.replica.util.ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME;

public class ReplicaSourceBroker extends MutativeRoleBroker {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private final ScheduledExecutorService heartBeatPoller = Executors.newSingleThreadScheduledExecutor();

    private final ReplicaEventReplicator replicaEventReplicator;
    private final ReplicaSequencer replicaSequencer;
    private final ReplicaReplicationDestinationSupplier destinationSupplier;
    private final ReplicaResynchronizer replicaResynchronizer;
    private final ReplicaPolicy replicaPolicy;
    private final ReplicaAckHelper replicaAckHelper;
    private ScheduledFuture<?> heartBeatScheduledFuture;

    public ReplicaSourceBroker(Broker broker, ReplicaRoleManagement management, ReplicaEventReplicator replicaEventReplicator,
            ReplicaSequencer replicaSequencer, ReplicaReplicationDestinationSupplier destinationSupplier,
            ReplicaResynchronizer replicaResynchronizer, ReplicaPolicy replicaPolicy) {
        super(broker, management);
        this.replicaEventReplicator = replicaEventReplicator;
        this.replicaSequencer = replicaSequencer;
        this.destinationSupplier = destinationSupplier;
        this.replicaResynchronizer = replicaResynchronizer;
        this.replicaPolicy = replicaPolicy;
        this.replicaAckHelper = new ReplicaAckHelper(next);
    }

    @Override
    public void start(ReplicaRole role, boolean resync) throws Exception {
        logger.info("Starting Source broker. " + (role == ReplicaRole.await_ack ? " Awaiting ack." : ""));

        if (resync && role != ReplicaRole.in_resync) {
            removeReplicationQueues();
        }
        initQueueProvider();
        replicaEventReplicator.initialize();

        if (role == ReplicaRole.in_resync || resync) {
            replicaResynchronizer.resynchronize(role, resync);
        } else {
            replicaEventReplicator.ensureDestinationsAreReplicated();
        }

        replicaSequencer.initialize();

        initializeHeartBeatSender();
    }

    @Override
    public void brokerServiceStarted(ReplicaRole role) {
        if (role == ReplicaRole.await_ack || role == ReplicaRole.in_resync) {
            stopAllConnections();
        }
    }

    @Override
    public void stop() throws Exception {
        replicaSequencer.deinitialize();
        super.stop();
        replicaEventReplicator.deinitialize();
    }

    @Override
    public void stopBeforeRoleChange(boolean force) throws Exception {
        logger.info("Stopping Source broker. Forced [{}]", force);
        stopAllConnections();
        if (force) {
            stopBeforeForcedRoleChange();
        } else {
            sendFailOverMessage();
        }
    }

    @Override
    public void startAfterRoleChange() throws Exception {
        logger.info("Starting Source broker after role change");
        startAllConnections();

        initQueueProvider();
        replicaEventReplicator.initialize();
        replicaSequencer.initialize();
        initializeHeartBeatSender();
        replicaSequencer.updateMainQueueConsumerStatus();
    }

    private void initializeHeartBeatSender() {
        if (replicaPolicy.getHeartBeatPeriod() > 0) {
            heartBeatScheduledFuture = heartBeatPoller.scheduleAtFixedRate(replicaEventReplicator::enqueueHeartBeatEvent,
                    replicaPolicy.getHeartBeatPeriod(), replicaPolicy.getHeartBeatPeriod(), TimeUnit.MILLISECONDS);
        }
    }


    private void stopBeforeForcedRoleChange() throws Exception {
        updateBrokerRole(ReplicaRole.replica);
        completeBeforeRoleChange();
    }

    private void completeBeforeRoleChange() throws Exception {
        replicaSequencer.deinitialize();
        if (heartBeatScheduledFuture != null) {
            heartBeatScheduledFuture.cancel(true);
        }
        removeReplicationQueues();

        onStopSuccess();
    }

    private void initQueueProvider() {
        destinationSupplier.initialize();
        destinationSupplier.initializeSequenceQueue();
    }

    private void sendFailOverMessage() throws Exception {
        ConnectionContext connectionContext = createConnectionContext();

        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        super.beginTransaction(connectionContext, tid);
        try {
            replicaEventReplicator.enqueueFailOverEvent(connectionContext, tid);
            updateBrokerRole(connectionContext, tid, ReplicaRole.await_ack);
            super.commitTransaction(connectionContext, tid, true);
        } catch (Exception e) {
            super.rollbackTransaction(connectionContext, tid);
            logger.error("Failed to send fail over message", e);
            throw e;
        }
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        Subscription subscription = super.addConsumer(context, consumerInfo);
        replicaEventReplicator.replicateAddConsumer(context, consumerInfo, context.getClientId());

        if (ReplicaSupport.isMainReplicationQueue(consumerInfo.getDestination())) {
            replicaSequencer.updateMainQueueConsumerStatus();
        }

        return subscription;
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        super.removeConsumer(context, consumerInfo);
        replicaEventReplicator.replicateRemoveConsumer(context, consumerInfo);
        if (ReplicaSupport.isMainReplicationQueue(consumerInfo.getDestination())) {
            replicaSequencer.updateMainQueueConsumerStatus();
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo subscriptionInfo) throws Exception {
        super.removeSubscription(context, subscriptionInfo);
        replicaEventReplicator.replicateRemoveSubscription(context, subscriptionInfo);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        super.commitTransaction(context, xid, onePhase);
        replicaEventReplicator.replicateCommitTransaction(context, xid, onePhase);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        int id = super.prepareTransaction(context, xid);
        replicaEventReplicator.replicatePrepareTransaction(context, xid);
        return id;
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.rollbackTransaction(context, xid);
        replicaEventReplicator.replicateRollbackTransaction(context, xid);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final ConnectionContext connectionContext = producerExchange.getConnectionContext();
        if (!replicaEventReplicator.needToReplicateSend(connectionContext, messageSend)) {
            super.send(producerExchange, messageSend);
            return;
        }

        boolean isInternalTransaction = false;
        TransactionId transactionId = null;
        if (messageSend.getTransactionId() != null && !messageSend.getTransactionId().isXATransaction()) {
            transactionId = messageSend.getTransactionId();
        } else if (messageSend.getTransactionId() == null) {
            transactionId = new LocalTransactionId(new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
            if (connectionContext.getTransactions() == null) {
                connectionContext.setTransactions(new ConcurrentHashMap<>());
            }
            super.beginTransaction(connectionContext, transactionId);
            messageSend.setTransactionId(transactionId);
            isInternalTransaction = true;
        }
        try {
            super.send(producerExchange, messageSend);
            if (isInternalTransaction) {
                super.commitTransaction(connectionContext, transactionId, true);
            }
        } catch (Exception e) {
            if (isInternalTransaction) {
                super.rollbackTransaction(connectionContext, transactionId);
            }
            throw e;
        }
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.beginTransaction(context, xid);
        replicaEventReplicator.replicateBeginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        super.forgetTransaction(context, transactionId);
        replicaEventReplicator.replicateForgetTransaction(context, transactionId);
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary)
            throws Exception {
        Destination newDestination = super.addDestination(context, destination, createIfTemporary);
        replicaEventReplicator.replicateDestinationCreation(context, destination);
        return newDestination;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        replicaEventReplicator.replicateDestinationRemoval(context, destination);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (ack.isDeliveredAck() || ack.isUnmatchedAck() || ack.isExpiredAck()) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        ConnectionContext connectionContext = consumerExchange.getConnectionContext();

        if (MAIN_REPLICATION_QUEUE_NAME.equals(ack.getDestination().getPhysicalName())) {
            LocalTransactionId transactionId = new LocalTransactionId(
                    new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

            super.beginTransaction(connectionContext, transactionId);
            ack.setTransactionId(transactionId);

            boolean failover = false;
            try {
                List<MessageReference> ackedMessageList = replicaSequencer.acknowledge(consumerExchange, ack);

                for (MessageReference mr : ackedMessageList) {
                    ActiveMQMessage message = (ActiveMQMessage) mr.getMessage();
                    ReplicaEventType eventType =
                            ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
                    if (eventType == ReplicaEventType.FAIL_OVER) {
                        failover = true;
                        break;
                    }
                }

                if (failover) {
                    updateBrokerRole(connectionContext, transactionId, ReplicaRole.replica);
                }

                super.commitTransaction(connectionContext, transactionId, true);
            } catch (Exception e) {
                super.rollbackTransaction(connectionContext, transactionId);
                logger.error("Failed to send broker fail over state", e);
                throw e;
            }
            if (failover) {
                completeBeforeRoleChange();
            }

            return;
        }

        PrefetchSubscription subscription = getDestinations(ack.getDestination()).stream().findFirst()
                .map(Destination::getConsumers).stream().flatMap(Collection::stream)
                .filter(c -> c.getConsumerInfo().getConsumerId().equals(ack.getConsumerId()))
                .findFirst().filter(PrefetchSubscription.class::isInstance).map(PrefetchSubscription.class::cast)
                .orElse(null);
        if (subscription == null) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        if (!replicaEventReplicator.needToReplicateAck(connectionContext, ack, subscription)) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        List<String> messageIdsToAck = getMessageIdsToAck(ack, subscription);
        if (messageIdsToAck == null || messageIdsToAck.isEmpty()) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        boolean isInternalTransaction = false;
        TransactionId transactionId = null;

        if (!ack.isPoisonAck()) {
            if (ack.getTransactionId() != null && !ack.getTransactionId().isXATransaction()) {
                transactionId = ack.getTransactionId();
            } else if (ack.getTransactionId() == null) {
                transactionId = new LocalTransactionId(new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
                super.beginTransaction(connectionContext, transactionId);
                ack.setTransactionId(transactionId);
                isInternalTransaction = true;
            }
        }

        try {
            super.acknowledge(consumerExchange, ack);
            replicaEventReplicator.replicateAck(connectionContext, ack, subscription, transactionId, messageIdsToAck,
                    connectionContext.getClientId());
            if (isInternalTransaction) {
                super.commitTransaction(connectionContext, transactionId, true);
            }
        } catch (Exception e) {
            if (isInternalTransaction) {
                super.rollbackTransaction(connectionContext, transactionId);
            }
            throw e;
        }
    }

    private List<String> getMessageIdsToAck(MessageAck ack, PrefetchSubscription subscription) {
        List<MessageReference> messagesToAck = replicaAckHelper.getMessagesToAck(ack, subscription);
        if (messagesToAck == null) {
            return null;
        }
        return messagesToAck.stream()
                .filter(MessageReference::isPersistent)
                .map(MessageReference::getMessageId)
                .map(MessageId::toProducerKey)
                .collect(Collectors.toList());
    }

    @Override
    public void queuePurged(ConnectionContext context, ActiveMQDestination destination) {
        super.queuePurged(context, destination);
        if(!ReplicaSupport.isReplicationDestination(destination)) {
            replicaEventReplicator.replicateQueuePurged(context, destination);
        } else {
            logger.error("Replication queue was purged {}", destination.getPhysicalName());
        }
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        super.messageExpired(context, message, subscription);
        replicaEventReplicator.replicateMessageExpired(context, message);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription, Throwable poisonCause) {
        if(ReplicaSupport.isReplicationDestination(messageReference.getMessage().getDestination())) {
            logger.error("A replication event is being sent to DLQ. It shouldn't even happen: " + messageReference.getMessage(), poisonCause);
            return false;
        }

        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }
}
