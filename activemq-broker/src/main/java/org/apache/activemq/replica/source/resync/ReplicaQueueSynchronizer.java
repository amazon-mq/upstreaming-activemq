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
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.DestinationExtractor;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.apache.activemq.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

class ReplicaQueueSynchronizer {

    private final Logger logger = LoggerFactory.getLogger(ReplicaQueueSynchronizer.class);

    private final Broker broker;
    private final ReplicaEventReplicator replicator;
    private final ActiveMQQueue destination;
    private final ConnectionContext connectionContext;
    private final AtomicInteger processedCount = new AtomicInteger();
    private final MessageStore messageStore;

    private ReplicaQueueSynchronizer(Broker broker, ReplicaEventReplicator replicator, ConnectionContext connectionContext, ActiveMQQueue destination) {
        this.broker = broker;
        this.replicator = replicator;
        this.destination = destination;
        this.connectionContext = connectionContext;
        Queue queue = broker.getDestinations(destination).stream().findFirst().map(DestinationExtractor::extractQueue).orElseThrow();
        messageStore = queue.getMessageStore();
    }

    static void resynchronize(Broker broker, ReplicaEventReplicator replicator, ConnectionContext connectionContext, ActiveMQQueue destination) throws Exception {
        new ReplicaQueueSynchronizer(broker, replicator, connectionContext, destination).resynchronize();
    }

    private void resynchronize() throws Exception {
        logger.info("Resyncronizing: " + destination);

        replicator.replicateDestinationRemoval(broker.getAdminConnectionContext(), destination);
        replicator.replicateDestinationCreation(broker.getAdminConnectionContext(), destination);

        messageStore.recover(new RecoveryListener());
    }

    private class RecoveryListener extends ReplicaMessageRecoveryListener {
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
