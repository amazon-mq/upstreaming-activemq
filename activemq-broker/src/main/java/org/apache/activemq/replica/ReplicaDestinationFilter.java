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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.virtual.CompositeDestinationFilter;
import org.apache.activemq.broker.region.virtual.VirtualTopicInterceptor;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.ReplicaRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaDestinationFilter extends DestinationFilter {
    private final boolean nextIsComposite;
    private final boolean nextIsVirtualTopic;

    private final ReplicaEventReplicator replicaEventReplicator;
    private final ReplicaRoleManagementBroker roleManagementBroker;

    private final Logger logger = LoggerFactory.getLogger(ReplicaDestinationFilter.class);

    public ReplicaDestinationFilter(Destination next, ReplicaEventReplicator replicaEventReplicator, ReplicaRoleManagementBroker roleManagementBroker) {
        super(next);
        this.nextIsComposite = this.next != null && this.next instanceof CompositeDestinationFilter;
        this.nextIsVirtualTopic = this.next != null && this.next instanceof VirtualTopicInterceptor;

        this.replicaEventReplicator = replicaEventReplicator;
        this.roleManagementBroker = roleManagementBroker;
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        if(ReplicaRole.source == roleManagementBroker.getRole()) {
            super.send(producerExchange, messageSend);
            if(!nextIsComposite) {
                // don't replicate composite destination
                replicateSend(producerExchange, messageSend);
            }
        } else {
            if(nextIsComposite) {
                // we jump over CompositeDestinationFilter as we don't want to fan out composite destinations on the replica side
                ((CompositeDestinationFilter) getNext()).getNext().send(producerExchange, messageSend);
            } else if (nextIsVirtualTopic) {
                // Do not fanout Virtual Topic messages to consumer queues. Instead, let the source broker replicate those messages.
                // Ensures consistency on how messages are fanned out on source and replica side.
                ((VirtualTopicInterceptor) getNext()).getNext().send(producerExchange, messageSend);
            } else {
                super.send(producerExchange, messageSend);
            }
        }
    }

    @Override
    public boolean canGC() {
        if (ReplicaRole.source == roleManagementBroker.getRole()) {
            return super.canGC();
        }
        return false;
    }

    private void replicateSend(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final ConnectionContext connectionContext = producerExchange.getConnectionContext();
        if (!replicaEventReplicator.needToReplicateSend(connectionContext, messageSend)) {
            return;
        }

        TransactionId transactionId = null;
        if (messageSend.getTransactionId() != null && !messageSend.getTransactionId().isXATransaction()) {
            transactionId = messageSend.getTransactionId();
        }

        replicaEventReplicator.replicateSend(connectionContext, messageSend, transactionId);
    }
}