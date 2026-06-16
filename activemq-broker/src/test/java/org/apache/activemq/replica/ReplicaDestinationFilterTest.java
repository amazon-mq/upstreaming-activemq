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
import org.apache.activemq.broker.region.virtual.CompositeDestinationFilter;
import org.apache.activemq.broker.region.virtual.VirtualTopicInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.Response;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.state.CommandVisitor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.JMSException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaDestinationFilterTest {
    @Mock
    private Destination destination;

    @Mock
    private ActiveMQDestination activeMQDestination;

    @Mock
    private ReplicaEventReplicator replicaEventReplicator;

    @Mock
    private ReplicaRoleManagementBroker roleManagementBroker;

    @Mock
    private ProducerBrokerExchange producerExchange;

    @Mock
    private ConnectionContext connectionContext;

    private Message message;

    @Before
    public void setUp() {
        when(producerExchange.getConnectionContext()).thenReturn(connectionContext);

        message = new StubMessage();
        message.setDestination(activeMQDestination);
        when(replicaEventReplicator.needToReplicateSend(connectionContext, message)).thenReturn(true); // Needs to replicate unless stated otherwise
    }

    @Test
    public void givenIsSourceWhenSendMessageThenReplicateSend() throws Exception {
        setIsSource();

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(destination).send(producerExchange, message);
        verify(replicaEventReplicator).replicateSend(eq(connectionContext), eq(message), any());
    }

    @Test
    public void givenIsSourceWhenReplicationNotNeededThenDoNotReplicate() throws Exception {
        setIsSource();

        when(replicaEventReplicator.needToReplicateSend(connectionContext, message)).thenReturn(false);

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(destination).send(producerExchange, message);
        verify(replicaEventReplicator, never()).replicateSend(any(), any(), any());
    }

    @Test
    public void givenIsSourceWhenCompositeDestinationThenDoNotReplicate() throws Exception {
        setIsSource();

        CompositeDestinationFilter compositeDestination = mock(CompositeDestinationFilter.class);

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(compositeDestination, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(compositeDestination).send(producerExchange, message);
        verify(replicaEventReplicator, never()).replicateSend(any(), any(), any());
    }

    @Test
    public void givenIsReplicaWhenSendMessageThenDelegateToNext() throws Exception {
        setIsReplica();

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(destination).send(producerExchange, message);
        verify(replicaEventReplicator, never()).replicateSend(any(), any(), any());
    }

    @Test
    public void givenIsReplicaWhenCompositeDestinationThenSkipCompositeFilter() throws Exception {
        setIsReplica();

        CompositeDestinationFilter compositeDestination = mock(CompositeDestinationFilter.class);
        Destination innerDestination = mock(Destination.class);
        when(compositeDestination.getNext()).thenReturn(innerDestination);

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(compositeDestination, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(innerDestination).send(producerExchange, message);
        verify(compositeDestination, never()).send(producerExchange, message);
    }

    @Test
    public void givenIsSourceWhenSendWithLocalTransactionThenPassTransactionId() throws Exception {
        setIsSource();

        LocalTransactionId txId = new LocalTransactionId(new ConnectionId("conn"), 1);
        message.setTransactionId(txId);

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(replicaEventReplicator).replicateSend(connectionContext, message, txId);
    }

    @Test
    public void givenIsSourceWhenCanGCThenDelegateToNext() {
        setIsSource();
        when(destination.canGC()).thenReturn(true);

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
        assertTrue(filter.canGC());
    }

    @Test
    public void givenIsReplicaWhenCanGCThenReturnFalse() {
        setIsReplica();

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(destination, replicaEventReplicator, roleManagementBroker);
        assertFalse(filter.canGC());
    }

    @Test
    public void givenIsReplicaWhenIsVirtualTopicThenDoNotSkipVirtualTopicInterceptor() throws Exception {
        setIsReplica();

        VirtualTopicInterceptor virtualTopicInterceptor = mock(VirtualTopicInterceptor.class);
        Destination innerDestination = mock(Destination.class);
        when(virtualTopicInterceptor.getNext()).thenReturn(innerDestination);

        ReplicaDestinationFilter filter = new ReplicaDestinationFilter(virtualTopicInterceptor, replicaEventReplicator, roleManagementBroker);
        filter.send(producerExchange, message);

        verify(innerDestination).send(producerExchange, message);
        verify(virtualTopicInterceptor, never()).send(producerExchange, message);
    }

    private void setIsSource() {
        when(roleManagementBroker.getRole()).thenReturn(ReplicaRole.source);
    }

    private void setIsReplica() {
        when(roleManagementBroker.getRole()).thenReturn(ReplicaRole.replica);
    }

    private static final class StubMessage extends Message {
        @Override
        public Message copy() {
            return new StubMessage();
        }

        @Override
        public void clearBody() throws JMSException {}

        @Override
        public void storeContent() {}

        @Override
        public void storeContentAndClear() {}

        @Override
        public Response visit(CommandVisitor visitor) throws Exception {
            return new Response();
        }

        @Override
        public byte getDataStructureType() {
            return 0;
        }
    }
}
