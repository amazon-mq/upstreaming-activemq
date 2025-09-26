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
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.replica.source.ReplicaEventReplicator;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaDestinationInterceptorTest {

    @Mock
    private ReplicaEventReplicator mockReplicaEventReplicator;

    @Mock
    private ReplicaRoleManagementBroker mockRoleManagementBroker;

    private ReplicaDestinationInterceptor interceptor;

    @Before
    public void setUp() {
        interceptor = new ReplicaDestinationInterceptor(mockReplicaEventReplicator, mockRoleManagementBroker);
    }

    @Test
    public void testInterceptReplicationQueueDestination() {
        ReplicaSupport.REPLICATION_QUEUE_NAMES.stream()
                .map(this::fakeQueue)
                .forEach(queue -> {
                    testIntercept(queue);
                    assertExpireMessagesPeriodOverridden(queue);
                });
    }

    @Test
    public void testInterceptReplicationTopicDestination() {
        ReplicaSupport.REPLICATION_TOPIC_NAMES.stream()
                .map(this::fakeTopic)
                .forEach(topic -> {
                    testIntercept(topic);
                    assertExpireMessagesPeriodOverridden(topic);
                });
    }

    @Test
    public void testInterceptNonReplicationQueue() {
        final Queue queue = fakeQueue("some.queue");
        testIntercept(queue);
        assertExpireMessagesPeriodNotOverridden(queue);
    }

    @Test
    public void testInterceptNonReplicationTopic() {
        final Topic topic = fakeTopic("some.topic");
        testIntercept(topic);
        assertExpireMessagesPeriodNotOverridden(topic);
    }

    @Test
    public void testInterceptDestinationFilterWithReplicationQueue() {
        ReplicaSupport.REPLICATION_QUEUE_NAMES.stream()
                .map(this::fakeQueue)
                .map(DestinationFilter::new)
                .forEach(this::testIntercept);

        // DestinationFilter does not have an ExpireMessagesPeriod
    }

    @Test
    public void testInterceptDestinationFilter() {
        final DestinationFilter destinationFilter = new DestinationFilter(fakeQueue("fake.queue"));
        testIntercept(destinationFilter);

        // DestinationFilter does not have an ExpireMessagesPeriod
    }

    private void testIntercept(final Destination destination) {
        Destination result = interceptor.intercept(destination);

        assertNotNull(result);
        assertTrue(result instanceof ReplicaDestinationFilter);

        // Resulting ReplicaDestinationFilter must wrap the original destination
        final ReplicaDestinationFilter resultingReplicaDestination = (ReplicaDestinationFilter) result;
        assertEquals(resultingReplicaDestination.getNext(), destination);
    }

    private void assertExpireMessagesPeriodOverridden(final BaseDestination destination) {
        verify(destination).setExpireMessagesPeriod(0L);
    }

    private void assertExpireMessagesPeriodNotOverridden(final BaseDestination destination) {
        verify(destination, never()).setExpireMessagesPeriod(0L);
    }

    private Queue fakeQueue(final String name) {
        final Queue queue = mock(Queue.class);
        when(queue.getActiveMQDestination()).thenReturn(new ActiveMQQueue(name));
        return queue;
    }

    private Topic fakeTopic(final String name) {
        final Topic topic = mock(Topic.class);
        when(topic.getActiveMQDestination()).thenReturn(new ActiveMQTopic(name));
        return topic;
    }

    @Test
    public void testRemove() {
        // ReplicaDestinationInterceptor.remove() is no-op.
        interceptor.remove(mock(Destination.class));
    }

    @Test
    public void testCreate() throws Exception {
        // ReplicaDestinationInterceptor.create() is no-op.
        interceptor.create(
                mock(Broker.class),
                mock(ConnectionContext.class),
                mock(ActiveMQDestination.class));
    }
}