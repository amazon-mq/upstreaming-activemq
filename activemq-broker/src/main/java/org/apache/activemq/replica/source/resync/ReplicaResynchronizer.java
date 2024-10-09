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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.ReplicaReplicationDestinationSupplier;
import org.apache.activemq.replica.ReplicaRoleManagement;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.storage.ReplicaResynchronizationStorage;
import org.apache.activemq.replica.storage.ResyncInfo;
import org.apache.activemq.replica.util.DummyConnection;
import org.apache.activemq.replica.util.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ReplicaResynchronizer {

    private static final String RESYNC_CONSUMER_CLIENT_ID = "DUMMY_RESYNC_CONSUMER";
    private final Logger logger = LoggerFactory.getLogger(ReplicaResynchronizer.class);
    private final Broker broker;
    private final ReplicaEventReplicator replicator;
    private final ReplicaRoleManagement management;
    private final ReplicaReplicationDestinationSupplier destinationSupplier;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;

    public ReplicaResynchronizer(Broker broker, ReplicaEventReplicator replicator, ReplicaRoleManagement management,
            ReplicaReplicationDestinationSupplier destinationSupplier, ReplicaInternalMessageProducer replicaInternalMessageProducer) {
        this.broker = broker;
        this.replicator = replicator;
        this.management = management;
        this.destinationSupplier = destinationSupplier;
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
    }

    public void resynchronize(ReplicaRole role, boolean resync) throws Exception {
        destinationSupplier.initializeResynchronizationQueue();
        ReplicaResynchronizationStorage storage = new ReplicaResynchronizationStorage(broker, destinationSupplier, replicaInternalMessageProducer);

        ConnectionContext connectionContext = broker.getAdminConnectionContext().copy();
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }
        connectionContext.setClientId(RESYNC_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection());

        if (resync && role != ReplicaRole.in_resync) {
            sendResetMessage(connectionContext);
        }

        ResyncInfo restoreResyncInfo = storage.initialize(connectionContext);

        List<ActiveMQDestination> destinations = broker.getDurableDestinations().stream().sorted((v1, v2) -> {
            int result = Boolean.compare(v1.isQueue(), v1.isQueue());
            if (result != 0) {
                return result;
            }
            return v1.getPhysicalName().compareTo(v2.getPhysicalName());
        }).collect(Collectors.toList());

        boolean found = false;
        for (ActiveMQDestination destination : destinations) {
            if (ReplicaSupport.isReplicationDestination(destination)) {
                continue;
            }

            MessageId restoreMessageId = null;
            if (restoreResyncInfo != null && !found) {
                if (restoreResyncInfo.getDestination().equals(destination)) {
                    found = true;
                    restoreMessageId = restoreResyncInfo.getMessageId();
                } else {
                    continue;
                }
            }

            if (destination.isQueue()) {
                ReplicaQueueSynchronizer.resynchronize(broker, replicator, storage, connectionContext,
                        (ActiveMQQueue) destination, restoreMessageId);
            }
            if (destination.isTopic()) {
                ReplicaTopicSynchronizer.resynchronize(broker, replicator, storage, connectionContext,
                        (ActiveMQTopic) destination, restoreMessageId);
            }
        }

        management.updateBrokerRole(connectionContext, null, ReplicaRole.source);
    }

    private void sendResetMessage(ConnectionContext connectionContext) throws Exception {
        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        broker.beginTransaction(connectionContext, tid);
        try {
            replicator.enqueueResetEvent(connectionContext, tid);
            management.updateBrokerRole(connectionContext, tid, ReplicaRole.in_resync);
            broker.commitTransaction(connectionContext, tid, true);
        } catch (Exception e) {
            broker.rollbackTransaction(connectionContext, tid);
            logger.error("Failed", e);
            throw e;
        }
    }
}
