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
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public abstract class MutativeRoleBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(MutativeRoleBroker.class);

    private final ReplicaRoleManagement management;

    public MutativeRoleBroker(Broker broker, ReplicaRoleManagement management) {
        super(broker);
        this.management = management;
    }

    public abstract void start(ReplicaRole role) throws Exception;

    protected abstract void stopBeforeRoleChange(boolean force) throws Exception;

    protected abstract void startAfterRoleChange() throws Exception;

    public abstract void brokerServiceStarted(ReplicaRole role);

    public void updateBrokerRole(ReplicaRole role) throws Exception {
        ConnectionContext connectionContext = createConnectionContext();
        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        super.beginTransaction(connectionContext, tid);
        try {
            updateBrokerRole(connectionContext, tid, role);
            super.commitTransaction(connectionContext, tid, true);
        } catch (Exception e) {
            super.rollbackTransaction(connectionContext, tid);
            logger.error("Failed to ack fail over message", e);
            throw e;
        }
    }

    protected void updateBrokerRole(ConnectionContext connectionContext, TransactionId tid, ReplicaRole role) throws Exception {
        management.updateBrokerRole(connectionContext, tid, role);
    }

    protected void stopAllConnections() {
        management.stopAllConnections();
    }

    protected void startAllConnections() throws Exception {
        management.startAllConnections();
    }

    protected void removeReplicationQueues() throws Exception {
        for (String queueName : ReplicaSupport.DELETABLE_REPLICATION_DESTINATION_NAMES) {
            super.removeDestination(createConnectionContext(), new ActiveMQQueue(queueName), 1000);
        }
    }

    protected void onStopSuccess() throws Exception {
        management.onStopSuccess();
    }

    protected ConnectionContext createConnectionContext() {
        ConnectionContext connectionContext = getAdminConnectionContext().copy();
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        return connectionContext;
    }
}
