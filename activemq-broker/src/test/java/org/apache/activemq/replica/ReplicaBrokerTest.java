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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.junit.Ignore;
import org.junit.Test;

public class ReplicaBrokerTest {

    @Test
    public void requiresConnectionFactory() {
        var broker = mock(Broker.class);

        var exception = catchThrowable(() -> new ReplicaBroker(broker,null));

        assertThat(exception).isExactlyInstanceOf(NullPointerException.class)
            .hasMessage("Need connection details of replica source for this broker");
    }

    @Test
    public void canInitialize() {
        var broker = mock(Broker.class);
        var connectionFactory = new ActiveMQConnectionFactory();

        new ReplicaBroker(broker, connectionFactory);
    }

    @Test
    @Ignore
    public void connectsToSourceBroker() {
        fail("Needs writing");
    }
}
