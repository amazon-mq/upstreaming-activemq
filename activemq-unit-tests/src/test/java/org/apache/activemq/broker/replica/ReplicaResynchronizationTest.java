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
package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.util.ReplicaRole;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.io.File;
import java.util.Arrays;

public class ReplicaResynchronizationTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.setClientID("CLIENT_ID");
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.setClientID("CLIENT_ID");
        secondBrokerConnection.start();
    }

    @Override
    protected void startFirstBroker() throws Exception {
        File dir = firstBroker.getBrokerDataDirectory();
        if (dir != null) {
            IOHelper.deleteChildren(dir);
        }
        super.startFirstBroker();
    }

    @Override
    protected void startSecondBroker() throws Exception {
        File dir = secondBroker.getBrokerDataDirectory();
        if (dir != null) {
            IOHelper.deleteChildren(dir);
        }
        super.startSecondBroker();
    }

    @After
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        super.tearDown();
    }
    @Test
    public void testQueueResync() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        int numberOfMessages = 10_000;
        for (int i = 0; i < numberOfMessages; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText(Integer.toString(i));
            firstBrokerProducer.send(message);
        }

        waitForCondition(() -> {
            try {
                QueueViewMBean secondBrokerDestinationQueue = getQueueView(secondBroker, destination.getPhysicalName());
                assertEquals(numberOfMessages, secondBrokerDestinationQueue.getEnqueueCount());
            } catch (Exception urlException) {
                urlException.printStackTrace();
                throw new RuntimeException(urlException);
            }
        });

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        for (int i = 0; i < numberOfMessages; i++) {
            Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

            receivedMessage.acknowledge();
        }

        MBeanServer secondBrokerMbeanServer = secondBroker.getManagementContext().getMBeanServer();
        ObjectName secondBrokerViewMBeanName = assertRegisteredObjectName(secondBrokerMbeanServer, secondBroker.getBrokerObjectName().toString());
        BrokerViewMBean secondBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(secondBrokerMbeanServer, secondBrokerViewMBeanName, BrokerViewMBean.class, true);
        secondBrokerMBean.removeQueue(destination.getPhysicalName());

        assertEquals(secondBrokerMBean.getQueues().length, 0);

        firstBroker.stop();
        firstBroker.waitUntilStopped();
        firstBroker = createFirstBroker();
        firstBroker.start();
        firstBroker.waitUntilStarted();

        waitForCondition(() -> {
            try {
                QueueViewMBean secondBrokerDestinationQueue = getQueueView(secondBroker, destination.getPhysicalName());
                assertEquals(numberOfMessages, secondBrokerDestinationQueue.getEnqueueCount());
            } catch (Exception urlException) {
                urlException.printStackTrace();
                throw new RuntimeException(urlException);
            }
        });

        for (int i = 0; i < numberOfMessages; i++) {
            Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

            receivedMessage.acknowledge();
        }

        firstBrokerSession.close();
        secondBrokerSession.close();
    }
    @Test
    public void testTopicResync() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("TOPIC." + getDestinationString());
        String subName1 = "SUB1";
        String subName2 = "SUB2";

        Session firstBrokerSession = firstBrokerConnection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        TopicSubscriber firstBrokerDurableSubscriber = firstBrokerSession.createDurableSubscriber(topic, subName1);
        firstBrokerSession.createDurableSubscriber(topic, subName2).close();

        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(topic);

        int numberOfMessages = 10_000;
        for (int i = 0; i < numberOfMessages; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText(Integer.toString(i));
            firstBrokerProducer.send(message);
        }

        for (int i = 0; i < numberOfMessages; i++) {
            Message receivedMessage = firstBrokerDurableSubscriber.receive(SHORT_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());
            if (i % 2 == 0) {
                receivedMessage.acknowledge();
            }
        }

        waitForCondition(() -> {
            try {
                TopicViewMBean secondBrokerDestinationTopic = getTopicView(secondBroker, topic.getPhysicalName());
                assertEquals(numberOfMessages, secondBrokerDestinationTopic.getEnqueueCount());
            } catch (Exception urlException) {
                urlException.printStackTrace();
                throw new RuntimeException(urlException);
            }
        });

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber(topic, subName1);
        MessageConsumer secondBrokerConsumer2 = secondBrokerSession.createDurableSubscriber(topic, subName2);

        for (int i = 1; i < numberOfMessages; i += 2) {
            Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

            receivedMessage.acknowledge();
        }

        for (int i = 0; i < numberOfMessages; i++) {
            Message receivedMessage = secondBrokerConsumer2.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

            receivedMessage.acknowledge();
        }

        MBeanServer secondBrokerMbeanServer = secondBroker.getManagementContext().getMBeanServer();
        ObjectName secondBrokerViewMBeanName = assertRegisteredObjectName(secondBrokerMbeanServer, secondBroker.getBrokerObjectName().toString());
        BrokerViewMBean secondBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(secondBrokerMbeanServer, secondBrokerViewMBeanName, BrokerViewMBean.class, true);
        secondBrokerMBean.removeTopic(topic.getPhysicalName());

        assertEquals(Arrays.stream(secondBrokerMBean.getTopics())
                .map(ObjectName::toString)
                .peek(name -> System.out.println("topic name: " + name))
                .filter(name -> name.contains("destinationName=" + topic.getPhysicalName()))
                .count(), 0);

        secondBrokerConsumer.close();
        secondBrokerConsumer2.close();

        firstBroker.stop();
        firstBroker.waitUntilStopped();
        firstBroker = createFirstBroker();
        firstBroker.start();
        firstBroker.waitUntilStarted();

        waitForCondition(() -> {
            try {
                TopicViewMBean secondBrokerDestinationTopic = getTopicView(secondBroker, topic.getPhysicalName());
                assertEquals(numberOfMessages, secondBrokerDestinationTopic.getEnqueueCount());
            } catch (Exception urlException) {
                urlException.printStackTrace();
                throw new RuntimeException(urlException);
            }
        });

        secondBrokerConsumer = secondBrokerSession.createDurableSubscriber(topic, subName1);
        secondBrokerConsumer2 = secondBrokerSession.createDurableSubscriber(topic, subName2);

        for (int i = 1; i < numberOfMessages; i += 2) {
            Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

            receivedMessage.acknowledge();
        }

        for (int i = 0; i < numberOfMessages; i++) {
            Message receivedMessage = secondBrokerConsumer2.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

            receivedMessage.acknowledge();
        }

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Override
    protected BrokerService createFirstBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(true);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(firstBindAddress);
        answer.setDataDirectory(FIRST_KAHADB_DIRECTORY);
        answer.setBrokerName("firstBroker");

        ReplicaPlugin replicaPlugin = new ReplicaPlugin();
        replicaPlugin.setRole(ReplicaRole.source);
        replicaPlugin.setTransportConnectorUri(firstReplicaBindAddress);
        replicaPlugin.setOtherBrokerUri(secondReplicaBindAddress);
        replicaPlugin.setControlWebConsoleAccess(false);
        replicaPlugin.setHeartBeatPeriod(0);
        replicaPlugin.setSourceSendPeriod(100);
        replicaPlugin.setResyncBrokersOnStart(true);

        answer.setPlugins(new BrokerPlugin[]{replicaPlugin});
        answer.setSchedulerSupport(true);
        return answer;
    }
}
