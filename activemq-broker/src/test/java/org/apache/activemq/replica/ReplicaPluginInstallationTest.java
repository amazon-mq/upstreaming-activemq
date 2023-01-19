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
import org.apache.activemq.broker.BrokerService;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ReplicaPluginInstallationTest {

    private final BrokerService brokerService = mock(BrokerService.class);
    private final Broker broker = mock(Broker.class);
    ReplicaPlugin pluginUnderTest = new ReplicaPlugin();

    @Before
    public void setUp() {
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(brokerService.isUseJmx()).thenReturn(false);
    }

    @Test
    public void testInstallPluginWithDefaultRole() throws Exception {
        pluginUnderTest.setTransportConnectorUri("failover:(tcp://localhost:61616)");
        assertTrue(pluginUnderTest.installPlugin(broker) instanceof ReplicaSourceBroker);
        assertEquals(ReplicaRole.source, pluginUnderTest.getRole());
    }

    @Test
    public void testInstallPluginWithReplicaRole() throws Exception {
        pluginUnderTest.setRole(ReplicaRole.replica);
        pluginUnderTest.setOtherBrokerUri("failover:(tcp://localhost:61616)");
        assertTrue(pluginUnderTest.installPlugin(broker) instanceof ReplicaBroker);
    }

    @Test
    public void testInstallPluginWithDualRole() throws Exception {
        pluginUnderTest.setRole(ReplicaRole.dual);
        pluginUnderTest.setTransportConnectorUri("failover:(tcp://localhost:61616)");
        assertTrue(pluginUnderTest.installPlugin(broker) instanceof ReplicaBroker);
    }
}
