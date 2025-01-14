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
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.util.ReplicaInternalMessageProducer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReplicaBaseObjectStorage<T extends Serializable> extends ReplicaBaseStorage<T> {

    public ReplicaBaseObjectStorage(Broker broker, ReplicaInternalMessageProducer replicaInternalMessageProducer,
            ActiveMQQueue destination, String idGeneratorPrefix, String selector) {
        super(broker, replicaInternalMessageProducer, destination, idGeneratorPrefix, selector);
    }

    @SuppressWarnings("unchecked")
    public List<T> initialize(ConnectionContext connectionContext, boolean keepOnlyOneMessage) throws Exception {
        List<T> result = new ArrayList<>();
        for (Message message : super.initializeBase(connectionContext, keepOnlyOneMessage)) {
            result.add((T) ((ActiveMQObjectMessage) message).getObject());
        }
        return result;
    }

    @Override
    public void send(ConnectionContext connectionContext, TransactionId tid, T object, MessageId messageId) throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObject(object);
        message.setTransactionId(tid);
        message.setDestination(destination);
        message.setMessageId(messageId);
        message.setProducerId(replicationProducerId);
        message.setPersistent(true);
        message.setResponseRequired(false);

        send(connectionContext, message);
    }
}
