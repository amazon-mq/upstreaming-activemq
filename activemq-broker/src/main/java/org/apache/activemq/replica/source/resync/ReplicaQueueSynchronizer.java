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
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.storage.ReplicaResynchronizationStorage;

class ReplicaQueueSynchronizer extends ReplicaDestinationSynchronizer {

    private ReplicaQueueSynchronizer(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage,
            ConnectionContext connectionContext, ActiveMQQueue destination, MessageId restoreMessageId) {
        super(broker, replicator, storage, connectionContext, destination, restoreMessageId);
    }

    static void resynchronize(Broker broker, ReplicaEventReplicator replicator, ReplicaResynchronizationStorage storage,
            ConnectionContext connectionContext, ActiveMQQueue destination, MessageId restoreMessageId) throws Exception {
        new ReplicaQueueSynchronizer(broker, replicator, storage, connectionContext, destination, restoreMessageId).resynchronize();
    }

    @Override
    public void processMessage(Message message, TransactionId tid) throws Exception {
        replicator.replicateSend(connectionContext, message, tid);
    }
}
