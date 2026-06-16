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

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Ignore;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.UUID;

public class ReplicaPluginQueueTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    protected XAConnection firstBrokerXAConnection;
    protected XAConnection secondBrokerXAConnection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        firstBrokerXAConnection = firstBrokerXAConnectionFactory.createXAConnection();
        firstBrokerXAConnection.start();

        secondBrokerXAConnection = secondBrokerXAConnectionFactory.createXAConnection();
        secondBrokerXAConnection.start();
        waitUntilReplicationQueueHasConsumer(firstBroker);
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        if (firstBrokerXAConnection != null) {
            firstBrokerXAConnection.close();
            firstBrokerXAConnection = null;
        }
        if (secondBrokerXAConnection != null) {
            secondBrokerXAConnection.close();
            secondBrokerXAConnection = null;
        }

        super.tearDown();
    }

    public void testSendMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testAcknowledgeMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        receivedMessage = firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        receivedMessage.acknowledge();

        Thread.sleep(LONG_TIMEOUT);

        secondBrokerSession.close();
        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageTransactionCommit() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.commit();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageTransactionRollback() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.rollback();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionCommit() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.commit(xid, false);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionCommitOnReplica() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        XASession secondBrokerXaSession = secondBrokerXAConnection.createXASession();
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes = secondBrokerXaSession.getXAResource();
        xaRes.commit(xid, false);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerXaSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionRollback() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.rollback(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testPurge() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT * 2);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        MBeanServer mbeanServer = firstBroker.getManagementContext().getMBeanServer();
        String objectNameStr = firstBroker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+getDestinationString();
        ObjectName queueViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        QueueViewMBean proxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);
        proxy.purge();

        Thread.sleep(LONG_TIMEOUT);

        secondBrokerSession.close();
        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testExpireMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        firstBrokerProducer.setTimeToLive(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        Thread.sleep(LONG_TIMEOUT + SHORT_TIMEOUT);

        secondBrokerSession.close();
        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageVirtualTopic() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic virtualTopic = new ActiveMQTopic("VirtualTopic." + getDestinationString());
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(virtualTopic);

        Queue queueOne = new ActiveMQQueue("Consumer.One." + virtualTopic.getTopicName());
        Queue queueTwo = new ActiveMQQueue("Consumer.Two." + virtualTopic.getTopicName());

        // Messages to consumer queues are only replicated if there are consumers in the source broker
        firstBrokerSession.createConsumer(queueOne);
        firstBrokerSession.createConsumer(queueTwo);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumerOne = secondBrokerSession.createConsumer(queueOne);
        MessageConsumer secondBrokerConsumerTwo = secondBrokerSession.createConsumer(queueTwo);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumerOne.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        receivedMessage = secondBrokerConsumerTwo.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Ignore ("Skipped because Pause Queue event is not replicated")
    public void pauseQueueAndResume() throws Exception {

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean firstBrokerDestinationQueueView = getQueueView(firstBroker, destination.getPhysicalName());
        firstBrokerDestinationQueueView.pause();
        assertTrue(firstBrokerDestinationQueueView.isPaused());
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerDestinationQueueView = getQueueView(secondBroker, destination.getPhysicalName());
        assertTrue(secondBrokerDestinationQueueView.isPaused());

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerDestinationQueueView.resume();
        Thread.sleep(LONG_TIMEOUT);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testBrowseMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerDestinationQueueView = getQueueView(secondBroker, destination.getPhysicalName());
        assertEquals(secondBrokerDestinationQueueView.browseMessages().size(), 1);
        TextMessage destinationMessage = (TextMessage) secondBrokerDestinationQueueView.browseMessages().get(0);
        assertEquals(destinationMessage.getText(), getName());

        assertEquals(secondBrokerDestinationQueueView.getProducerCount(), 0);
        assertEquals(secondBrokerDestinationQueueView.getConsumerCount(), 0);

        Message receivedMessage = firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        receivedMessage.acknowledge();
        Thread.sleep(LONG_TIMEOUT);
        assertEquals(secondBrokerDestinationQueueView.getDequeueCount(), 1);
        firstBrokerSession.close();
    }

    public void testDeleteMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        MBeanServer secondBrokerMbeanServer = secondBroker.getManagementContext().getMBeanServer();
        ObjectName secondBrokerViewMBeanName = assertRegisteredObjectName(secondBrokerMbeanServer, secondBroker.getBrokerObjectName().toString());
        BrokerViewMBean secondBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(secondBrokerMbeanServer, secondBrokerViewMBeanName, BrokerViewMBean.class, true);
        assertEquals(secondBrokerMBean.getQueues().length, 1);
        assertEquals(Arrays.stream(secondBrokerMBean.getQueues())
                .map(ObjectName::toString)
                .filter(name -> name.contains(destination.getPhysicalName()))
                .count(), 1);

        MBeanServer firstBrokerMbeanServer = firstBroker.getManagementContext().getMBeanServer();
        ObjectName firstBrokerViewMBeanName = assertRegisteredObjectName(firstBrokerMbeanServer, firstBroker.getBrokerObjectName().toString());
        BrokerViewMBean firstBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(firstBrokerMbeanServer, firstBrokerViewMBeanName, BrokerViewMBean.class, true);
        firstBrokerMBean.removeQueue(destination.getPhysicalName());
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(secondBrokerMBean.getQueues().length, 0);

        firstBrokerSession.close();
    }

    public void testTemporaryQueueIsNotReplicated() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        TemporaryQueue tempQueue = firstBrokerSession.createTemporaryQueue();

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        String id = UUID.randomUUID().toString();
        message.setJMSReplyTo(tempQueue);
        message.setJMSCorrelationID(id);
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        MessageConsumer firstBrokerDestinationConsumer = firstBrokerSession.createConsumer(destination);
        Message firstBrokerMessageDestinationReceived = firstBrokerDestinationConsumer.receive(LONG_TIMEOUT);
        if (firstBrokerMessageDestinationReceived instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) firstBrokerMessageDestinationReceived;
            Destination replyBackQueue = textMessage.getJMSReplyTo();
            MessageProducer producer = firstBrokerSession.createProducer(replyBackQueue);

            TextMessage msg = firstBrokerSession.createTextMessage("Message Received : " + textMessage.getText());
            producer.send(msg);
        }

        MessageConsumer firstBrokerTempQueueConsumer = firstBrokerSession.createConsumer(tempQueue);
        Message firstBrokerMessageReceived = firstBrokerTempQueueConsumer.receive(LONG_TIMEOUT);
        assertNotNull(firstBrokerMessageReceived);
        assertTrue(((TextMessage) firstBrokerMessageReceived).getText().contains(getName()));

        String tempQueueJMXName = tempQueue.getQueueName().replaceAll(":", "_");
        MBeanServer firstBrokerMbeanServer = firstBroker.getManagementContext().getMBeanServer();
        ObjectName firstBrokerViewMBeanName = assertRegisteredObjectName(firstBrokerMbeanServer, firstBroker.getBrokerObjectName().toString());
        BrokerViewMBean firstBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(firstBrokerMbeanServer, firstBrokerViewMBeanName, BrokerViewMBean.class, true);
        assertEquals(firstBrokerMBean.getTemporaryQueues().length, 1);
        assertTrue(firstBrokerMBean.getTemporaryQueues()[0].toString().contains(tempQueueJMXName));

        MBeanServer secondBrokerMbeanServer = secondBroker.getManagementContext().getMBeanServer();
        ObjectName secondBrokerViewMBeanName = assertRegisteredObjectName(secondBrokerMbeanServer, secondBroker.getBrokerObjectName().toString());
        BrokerViewMBean secondBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(secondBrokerMbeanServer, secondBrokerViewMBeanName, BrokerViewMBean.class, true);
        assertEquals(secondBrokerMBean.getTemporaryQueues().length, 0);

        firstBrokerSession.close();
    }

    public void testAllVirtualTopicMessagesAreReplicated() throws Exception {
        final String virtualTopicName = "VirtualTopic." + getDestinationString();
        final String firstConsumerQueueName = "Consumer.One." + virtualTopicName;
        final String secondConsumerQueueName = "Consumer.Two." + virtualTopicName;

        final Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Topic virtualTopic = new ActiveMQTopic(virtualTopicName);

        final MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(virtualTopic);
        firstBrokerProducer.setDeliveryMode(javax.jms.DeliveryMode.PERSISTENT);

        firstBrokerSession.createConsumer(new ActiveMQQueue(firstConsumerQueueName));
        firstBrokerSession.createConsumer(new ActiveMQQueue(secondConsumerQueueName));

        final int messageCount = 100;

        for (int i = 0; i < messageCount; i++) {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setText(getName());
            firstBrokerProducer.send(msg);
        }

        waitUntilQueueExists(secondBroker, firstConsumerQueueName);
        waitUntilQueueExists(secondBroker, secondConsumerQueueName);
        waitUntilQueueHasMessages(secondBroker, firstConsumerQueueName, messageCount);
        waitUntilQueueHasMessages(secondBroker, secondConsumerQueueName, messageCount);

        firstBrokerSession.close();
    }

    public void testVirtualTopicFanOutAcksAreReplicated() throws Exception {
        // Ref.: https://t.corp.amazon.com/V2193145703

        final String virtualTopicName = "VirtualTopic." + getDestinationString();
        final String firstConsumerQueueName = "Consumer.One." + virtualTopicName;
        final String secondConsumerQueueName = "Consumer.Two." + virtualTopicName;

        final int messagesToPublish = 13;
        final int messagesToAcknowledge = 10;
        final int expectedRemainingMessages = messagesToPublish - messagesToAcknowledge;

        final Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Topic virtualTopic = new ActiveMQTopic(virtualTopicName);

        final MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(virtualTopic);
        firstBrokerProducer.setDeliveryMode(javax.jms.DeliveryMode.PERSISTENT);

        final MessageConsumer firstConsumer = firstBrokerSession.createConsumer(new ActiveMQQueue(firstConsumerQueueName));
        final MessageConsumer secondConsumer = firstBrokerSession.createConsumer(new ActiveMQQueue(secondConsumerQueueName));

        // Publish to the virtual topic
        for (int i = 0; i < messagesToPublish; i++) {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setText(getName());
            firstBrokerProducer.send(msg);
        }

        // Consume messages from the Consumer Queues
        for (int i = 0; i < messagesToAcknowledge; i++) {
            final Message firstMsg = firstConsumer.receive(SHORT_TIMEOUT);
            assertNotNull(firstMsg);
            firstMsg.acknowledge();

            final Message secondMsg = secondConsumer.receive(SHORT_TIMEOUT);
            assertNotNull(secondMsg);
            secondMsg.acknowledge();
        }

        // Consumer queues must have been consumed in the primary broker
        waitUntilQueueHasMessages(firstBroker, firstConsumerQueueName, expectedRemainingMessages);
        waitUntilQueueHasMessages(firstBroker, secondConsumerQueueName, expectedRemainingMessages);

        // Consumer queues must have been replicated to the replica broker
        waitUntilQueueExists(secondBroker, firstConsumerQueueName);
        waitUntilQueueExists(secondBroker, secondConsumerQueueName);

        // Primary broker replication backlog is empty (no more messages pending replication)
        waitUntilReplicationQueueIsEmpty(firstBroker);

        // The consumer queues state in the replica broker must match the one in the primary broker
        waitUntilQueueHasMessages(secondBroker, firstConsumerQueueName, expectedRemainingMessages);
        waitUntilQueueHasMessages(secondBroker, secondConsumerQueueName, expectedRemainingMessages);

        firstBrokerSession.close();
    }

    public void testDurableTopicConsumersAreReplicated() throws Exception {
        // Ref.: https://t.corp.amazon.com/V2193145703

        final String topicName = "MyDurableTopics." + getDestinationString();

        // Must be the same on Primary and Replica brokers
        final String firstConsumerName = "MyFirstConsumer";
        final String secondConsumerName = "MySecondConsumer";
        final String clientId = "MyClientId";

        final int messagesToPublish = 13;
        final int messagesToAcknowledge = 10;
        final int expectedRemainingMessages = messagesToPublish * 2 - messagesToAcknowledge;

        firstBrokerConnection.close(); // Close the connection so we can open again with the test's own client id
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.setClientID(clientId);
        firstBrokerConnection.start();
        final Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Topic topic = new ActiveMQTopic(topicName);

        final MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(topic);
        firstBrokerProducer.setDeliveryMode(javax.jms.DeliveryMode.PERSISTENT);

        final MessageConsumer firstConsumer = firstBrokerSession.createDurableConsumer(new ActiveMQTopic(topicName), firstConsumerName);
        final MessageConsumer secondConsumer = firstBrokerSession.createDurableConsumer(new ActiveMQTopic(topicName), secondConsumerName);

        // Publish to the topic
        for (int i = 0; i < messagesToPublish; i++) {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setText(getName());
            firstBrokerProducer.send(msg);
        }

        // Consume some messages
        for (int i = 0; i < messagesToAcknowledge; i++) {
            firstConsumer.receive();
            secondConsumer.receive();
        }

        // Publish another batch of messages
        for (int i = 0; i < messagesToPublish; i++) {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setText(getName());
            firstBrokerProducer.send(msg);
        }

        // Close first consumer, leave second consumer connected
        firstConsumer.close();

        // Consumer queues must have been replicated to the replica broker
        waitUntilTopicExists(secondBroker, topicName);

        // Primary broker replication backlog is empty (no more messages pending replication)
        waitUntilReplicationQueueIsEmpty(firstBroker);

        secondBrokerConnection.close(); // Will reopen the connection with the same client id used in the source broker
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.setClientID(clientId);
        secondBrokerConnection.start();
        final Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        final MessageConsumer replicaFirstConsumer = secondBrokerSession.createDurableConsumer(new ActiveMQTopic(topicName), firstConsumerName);
        final MessageConsumer replicaSecondConsumer = secondBrokerSession.createDurableConsumer(new ActiveMQTopic(topicName), secondConsumerName);

        waitForCondition("Pending messages to first durable consumer must have been replicated", () -> {
            for (int i = 0; i < expectedRemainingMessages; i++) {
                replicaFirstConsumer.receive();
            }
            return true;
        });

        waitForCondition("Pending messages to second durable consumer must have been replicated", () -> {
            for (int i = 0; i < expectedRemainingMessages; i++) {
                replicaSecondConsumer.receive();
            }
            return true;
        });

        firstBrokerSession.close();
    }
}
