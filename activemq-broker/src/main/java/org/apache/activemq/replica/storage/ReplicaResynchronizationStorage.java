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
package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.ReplicaReplicationDestinationSupplier;
import org.apache.activemq.replica.util.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.util.ReplicaRole;

import java.util.List;

public class ReplicaResynchronizationStorage extends ReplicaBaseStorage {

    public ReplicaResynchronizationStorage(Broker broker, ReplicaReplicationDestinationSupplier destinationSupplier,
            ReplicaInternalMessageProducer replicaInternalMessageProducer) {
        super(broker, replicaInternalMessageProducer, destinationSupplier.getResynchronizationQueue(),  "ReplicationPlugin.ReplicaResynchronizationStorage", null);
    }

    public ResyncInfo initialize(ConnectionContext connectionContext) throws Exception {
        List<Message> allMessages = super.initializeBase(connectionContext, true);

        if (allMessages.isEmpty()) {
            return null;
        }

        return (ResyncInfo) ((ActiveMQObjectMessage) allMessages.get(0)).getObject();
    }

    public void enqueue(ConnectionContext connectionContext, TransactionId tid, ResyncInfo resyncInfo) throws Exception {
        // before enqueue message, we acknowledge all messages currently in queue.
        acknowledgeAll(connectionContext, tid);

        send(connectionContext, tid, resyncInfo,
                new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
    }

    public void send(ConnectionContext connectionContext, TransactionId tid, ResyncInfo resyncInfo, MessageId messageId) throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObject(resyncInfo);
        message.setTransactionId(tid);
        message.setDestination(destination);
        message.setMessageId(messageId);
        message.setProducerId(replicationProducerId);
        message.setPersistent(true);
        message.setResponseRequired(false);

        send(connectionContext, message);
    }
}
