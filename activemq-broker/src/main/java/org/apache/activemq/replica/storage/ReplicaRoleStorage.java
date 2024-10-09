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
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.util.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationDestinationSupplier;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ReplicaRoleStorage extends ReplicaBaseStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaRoleStorage.class);

    public ReplicaRoleStorage(Broker broker, ReplicaReplicationDestinationSupplier destinationSupplier,
            ReplicaInternalMessageProducer replicaInternalMessageProducer) {
        super(broker, replicaInternalMessageProducer, destinationSupplier.getRoleQueue(),  "ReplicationPlugin.ReplicaFailOverStorage", null);
        this.replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    public ReplicaRole initialize(ConnectionContext connectionContext) throws Exception {
        List<ActiveMQTextMessage> allMessages = super.initializeBase(connectionContext, true);

        if (allMessages.isEmpty()) {
            return null;
        }

        return ReplicaRole.valueOf(allMessages.get(0).getText());
    }
}
