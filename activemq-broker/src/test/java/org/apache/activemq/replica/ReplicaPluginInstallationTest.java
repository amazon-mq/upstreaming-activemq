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
import org.apache.activemq.broker.ErrorBroker;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class ReplicaPluginInstallationTest {

    private BrokerService brokerService;
    private Broker broker;
    ReplicaPlugin pluginUnderTest = new ReplicaPlugin();

    @Before
    public void setUp() {
        brokerService = new BrokerService();
        broker = new ErrorBroker("Error broker name") {
            @Override
            public BrokerService getBrokerService() {
                return brokerService;
            }
        };
    }

    @Test
    public void testInstallPluginWithDefaultRole() throws Exception {
        pluginUnderTest.setTransportConnectorUri("failover:(tcp://localhost:61616)");
        assertTrue(pluginUnderTest.installPlugin(broker) instanceof ReplicaSourceBroker);
        assertEquals(ReplicaRole.source, pluginUnderTest.getRole());
    }

    @Test
    public void throwErrorWhenTransportConnectorUriNotSetWithDefaultRole() {
        Throwable exception = assertThrows(NullPointerException.class, () -> pluginUnderTest.installPlugin(broker));
        assertEquals("Need replication transport connection URI for this broker", exception.getMessage());
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

    @Test
    public void throwErrorWhenTransportConnectorUriNotSetWithDualRole() {
        pluginUnderTest.setRole(ReplicaRole.dual);
        Throwable exception = assertThrows(NullPointerException.class, () -> pluginUnderTest.installPlugin(broker));
        assertEquals("Need replication transport connection URI for this broker", exception.getMessage());
    }

}
