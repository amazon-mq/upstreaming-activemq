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
package org.apache.activemq.transport.amqp;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.transport.amqp.protocol.AmqpConnection;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ReplicaPluginAmqpConnectionTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaPluginAmqpConnectionTest.class);
    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    private final SpringSslContext sslContext = new SpringSslContext();
    private static final long LONG_TIMEOUT = 15000;

    public static final String PRIMARY_BROKER_CONFIG = "org/apache/activemq/transport/amqp/transport-protocol-test-primary.xml";
    public static final String REPLICA_BROKER_CONFIG = "org/apache/activemq/transport/amqp/transport-protocol-test-replica.xml";
    public static final String ACTIVE_MQ_TEXT_MESSAGE = "ActiveMQTextMessage";
    private final String protocol;
    protected BrokerService firstBroker;
    protected BrokerService secondBroker;
    private JmsConnection firstBrokerConnection;
    protected ActiveMQDestination destination;

    @Before
    public void setUp() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);
        final File classesDir = new File(AmqpConnection.class.getProtectionDomain().getCodeSource().getLocation().getFile());
        File keystore = new File(classesDir, "../../src/test/resources/keystore");
        final SpringSslContext sslContext = new SpringSslContext();
        sslContext.setKeyStore(keystore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(keystore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();
        System.setProperty("javax.net.ssl.trustStore", keystore.getCanonicalPath());
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", keystore.getCanonicalPath());
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);

        secondBroker =  setUpBrokerService(REPLICA_BROKER_CONFIG);
        secondBroker.getBroker().getBrokerService().setSslContext(sslContext);
        secondBroker.start();
        secondBroker.waitUntilStarted();

        firstBroker =  setUpBrokerService(PRIMARY_BROKER_CONFIG);
        firstBroker.getBroker().getBrokerService().setSslContext(sslContext);
        firstBroker.start();
        firstBroker.waitUntilStarted();

        destination = new ActiveMQQueue(getClass().getName());
    }

    @After
    public void tearDown() throws Exception {
        firstBrokerConnection.close();
        if (firstBroker != null) {
            try {
                firstBroker.stop();
                firstBroker.waitUntilStopped();
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
        if (secondBroker != null) {
            try {
                secondBroker.stop();
                secondBroker.waitUntilStopped();
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    @Parameterized.Parameters(name="protocol={0}")
    public static Collection<String[]> getTestParameters() {
        return Arrays.asList(new String[][] {
                {"amqp"} // {"amqp+ssl"}, {"amqp+nio+ssl"}, {"amqp+nio"}, {"amqp+ssl"},
        });
    }

    @Test
    public void testMessageSendAndReceive() throws Exception {
        TransportConnector firstBrokerConnector = firstBroker.getTransportConnectorByScheme(protocol);
        firstBrokerConnection = (JmsConnection) createConnectionFactory(firstBrokerConnector).createConnection();
        firstBrokerConnection.start();
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(ACTIVE_MQ_TEXT_MESSAGE);
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerQueueViewMBean = getQueueView(secondBroker, destination.getPhysicalName());
        // check that message is replicated
        assertEquals(secondBrokerQueueViewMBean.getEnqueueCount(), 1);

        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);
        Message receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        TextMessage msg = (TextMessage) receivedMessage;
        assertEquals(ACTIVE_MQ_TEXT_MESSAGE, msg.getText());

        receivedMessage.acknowledge();
        Thread.sleep(LONG_TIMEOUT);

        // check that ack is replicated and message is dispatched on the replica broker
        assertEquals(secondBrokerQueueViewMBean.getDequeueCount(), 1);
    }

    protected QueueViewMBean getQueueView(BrokerService broker, String queueName) throws MalformedObjectNameException {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+queueName;
        ObjectName queueViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);
    }

    private ObjectName assertRegisteredObjectName(MBeanServer mbeanServer, String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            System.out.println("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    private JmsConnectionFactory createConnectionFactory(TransportConnector connector) throws IOException, URISyntaxException {
        return new JmsConnectionFactory(protocol + "://localhost:" + connector.getConnectUri().getPort());
    }

    public ReplicaPluginAmqpConnectionTest(String protocol) {
        this.protocol = protocol;
    }

    protected BrokerService setUpBrokerService(String configurationUri) throws Exception {
        BrokerService broker = createBroker(configurationUri);
        broker.setPersistent(false);
        broker.setSslContext(sslContext);
        return broker;
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}
