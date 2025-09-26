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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaDestinationInterceptor implements DestinationInterceptor {

    private static final long NEVER_EXPIRE_MESSAGES = 0L;

    private final Logger logger = LoggerFactory.getLogger(ReplicaDestinationInterceptor.class);

    private final ReplicaEventReplicator replicaEventReplicator;
    private final ReplicaRoleManagementBroker roleManagementBroker;

    public ReplicaDestinationInterceptor(ReplicaEventReplicator replicaEventReplicator, ReplicaRoleManagementBroker roleManagementBroker) {
        this.replicaEventReplicator = replicaEventReplicator;
        this.roleManagementBroker = roleManagementBroker;
    }

    @Override
    public Destination intercept(Destination destination) {
        // Called when a Destination is added.

        // Do not try to expire messages from Replication Destinations:
        // Mitigates a race condition between Queue.expireMessages() and ReplicaResynchronizer.resynchronize() when the
        // ReplicaSourceBroker starts. Both threads race for calling synchronized methods in StoreQueueCursor and for
        // locking KahaDBStore.indexLock (in opposite orders).
        //
        // (see https://taskei.amazon.dev/tasks/AMQ-176060)
        //
        // Messages pushed to a Replication Destination won't expire anyway.
        if (ReplicaSupport.isReplicationDestination(destination.getActiveMQDestination())) {
            if (destination instanceof BaseDestination) { // Could be a DestinationFilter instead
                logger.info("Overriding Expire Message Period for destination '{}'", destination);
                ((BaseDestination) destination).setExpireMessagesPeriod(NEVER_EXPIRE_MESSAGES);
            }
        }

        return new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
    }

    @Override
    public void remove(Destination destination) {
        // Called when a Destination is removed
    }

    @Override
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
        // Called when a Consumer is created.
    }
}