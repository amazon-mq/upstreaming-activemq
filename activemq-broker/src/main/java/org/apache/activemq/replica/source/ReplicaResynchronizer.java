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
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.util.DestinationExtractor;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ReplicaResynchronizer {

    private final Logger logger = LoggerFactory.getLogger(ReplicaResynchronizer.class);
    private final Broker broker;
    private final ReplicaEventReplicator replicator;

    public ReplicaResynchronizer(Broker broker, ReplicaEventReplicator replicator) {
        this.broker = broker;
        this.replicator = replicator;
    }

    public void resynchronize() throws Exception {
        List<ActiveMQQueue> destinations = broker.getDurableDestinations().stream()
                .filter(ActiveMQDestination::isQueue)
                .map(ActiveMQQueue.class::cast)
                .collect(Collectors.toList());

        for (ActiveMQQueue desination : destinations) {
            if (ReplicaSupport.isReplicationDestination(desination)) {
                continue;
            }
            logger.info("Resyncronizing: " + desination);
            Queue queue = broker.getDestinations(desination).stream().findFirst().map(DestinationExtractor::extractQueue).orElseThrow();

            MessageStore messageStore = queue.getMessageStore();
            AtomicInteger i = new AtomicInteger();

            ConnectionContext connectionContext = broker.getAdminConnectionContext().copy();
            if (connectionContext.getTransactions() == null) {
                connectionContext.setTransactions(new ConcurrentHashMap<>());
            }
            messageStore.recover(new MessageRecoveryListener() {
                @Override
                public boolean recoverMessage(Message message) throws Exception {
                    int val = i.incrementAndGet();
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

                @Override
                public boolean recoverMessageReference(MessageId ref) throws Exception {
                    return false;
                }

                @Override
                public boolean hasSpace() {
                    return true;
                }

                @Override
                public boolean isDuplicate(MessageId ref) {
                    return false;
                }
            });
            messageStore.resetBatching();
        }
    }
}
