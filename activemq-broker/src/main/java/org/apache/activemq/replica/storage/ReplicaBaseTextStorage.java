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
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.util.ReplicaInternalMessageProducer;

import java.util.List;
import java.util.stream.Collectors;

public abstract class ReplicaBaseTextStorage extends ReplicaBaseStorage {

    public ReplicaBaseTextStorage(Broker broker, ReplicaInternalMessageProducer replicaInternalMessageProducer,
            ActiveMQQueue destination, String idGeneratorPrefix, String selector) {
        super(broker, replicaInternalMessageProducer, destination, idGeneratorPrefix, selector);
    }

    protected List<ActiveMQTextMessage> initialize(ConnectionContext connectionContext, boolean keepOnlyOneMessage) throws Exception {
        return super.initializeBase(connectionContext, keepOnlyOneMessage).stream()
                .map(ActiveMQTextMessage.class::cast).collect(Collectors.toList());
    }

    public void enqueue(ConnectionContext connectionContext, TransactionId tid, String message) throws Exception {
        // before enqueue message, we acknowledge all messages currently in queue.
        acknowledgeAll(connectionContext, tid);

        send(connectionContext, tid, message,
                new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
    }

    public void send(ConnectionContext connectionContext, TransactionId tid, String message, MessageId messageId) throws Exception {
        ActiveMQTextMessage seqMessage = new ActiveMQTextMessage();
        seqMessage.setText(message);
        seqMessage.setTransactionId(tid);
        seqMessage.setDestination(destination);
        seqMessage.setMessageId(messageId);
        seqMessage.setProducerId(replicationProducerId);
        seqMessage.setPersistent(true);
        seqMessage.setResponseRequired(false);

        send(connectionContext, seqMessage);
    }
}
