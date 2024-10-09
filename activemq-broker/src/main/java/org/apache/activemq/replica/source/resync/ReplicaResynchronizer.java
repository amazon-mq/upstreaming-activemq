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
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.replica.ReplicaRoleManagement;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ReplicaResynchronizer {

    private final Logger logger = LoggerFactory.getLogger(ReplicaResynchronizer.class);
    private final Broker broker;
    private final ReplicaEventReplicator replicator;
    private final ReplicaRoleManagement management;

    public ReplicaResynchronizer(Broker broker, ReplicaEventReplicator replicator, ReplicaRoleManagement management) {
        this.broker = broker;
        this.replicator = replicator;
        this.management = management;
    }

    public void resynchronize(ReplicaRole role, boolean resync) throws Exception {
        ConnectionContext connectionContext = broker.getAdminConnectionContext().copy();
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        if (resync && role != ReplicaRole.in_resync) {
            sendResetMessage(connectionContext);
        }

        List<ActiveMQQueue> destinations = broker.getDurableDestinations().stream()
                .filter(ActiveMQDestination::isQueue)
                .map(ActiveMQQueue.class::cast)
                .collect(Collectors.toList());

        for (ActiveMQQueue destination : destinations) {
            if (ReplicaSupport.isReplicationDestination(destination)) {
                continue;
            }

            ReplicaQueueSynchronizer.resynchronize(broker, replicator, connectionContext, destination);
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
