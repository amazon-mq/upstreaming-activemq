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
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.storage.ReplicaResynchronizationStorage;
import org.apache.activemq.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ReplicaDestinationSynchronizer implements ReplicaMessageRecoveryListener.RecoveryListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaDestinationSynchronizer.class);

    final Broker broker;
    final ReplicaEventReplicator replicator;
    final ConnectionContext connectionContext;
    final ActiveMQDestination destination;
    final MessageStore messageStore;
    private final ReplicaMessageRecoveryListener listener;

    ReplicaDestinationSynchronizer(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage,
            ConnectionContext connectionContext, ActiveMQDestination destination, MessageId restoreMessageId) {
        this.broker = broker;
        this.replicator = replicator;
        this.connectionContext = connectionContext;
        this.destination = destination;
        messageStore = broker.getDestinations(destination).stream().findFirst().map(Destination::getMessageStore).orElseThrow();
        listener = new ReplicaMessageRecoveryListener(broker, storage, connectionContext, destination, restoreMessageId, this);
    }

    void resynchronize() throws Exception {
        logger.info("Resyncronizing: " + destination);

        preRecover();

        messageStore.recover(listener);
        listener.commitIfNeeded();
    }

    void preRecover() throws Exception {
        replicator.replicateDestinationRemoval(connectionContext, destination, true);
        replicator.replicateDestinationCreation(connectionContext, destination);
    }
}
