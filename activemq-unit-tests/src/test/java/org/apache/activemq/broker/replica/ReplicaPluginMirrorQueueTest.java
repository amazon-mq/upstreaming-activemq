package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.activemq.util.Wait;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;

public class ReplicaPluginMirrorQueueTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    @Override
    protected void setUp() throws Exception {
        firstBroker = createFirstMirroredBroker();
        secondBroker = createSecondBroker();

        startFirstBroker();
        startSecondBroker();

        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        destination = createDestination();

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

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

        super.tearDown();
    }

    public void testSendMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

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

    private BrokerService createFirstMirroredBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(firstBindAddress);
        answer.setDataDirectory(FIRST_KAHADB_DIRECTORY);
        answer.setBrokerName("firstBroker");
        answer.setUseMirroredQueues(true);

        ReplicaPlugin replicaPlugin = new ReplicaPlugin();
        replicaPlugin.setRole(ReplicaRole.source);
        replicaPlugin.setTransportConnectorUri(firstReplicaBindAddress);
        replicaPlugin.setOtherBrokerUri(secondReplicaBindAddress);
        replicaPlugin.setControlWebConsoleAccess(false);
        replicaPlugin.setHeartBeatPeriod(0);

        answer.setPlugins(new BrokerPlugin[]{replicaPlugin});
        answer.setSchedulerSupport(true);
        return answer;
    }

    private void waitUntilReplicationQueueHasConsumer(BrokerService broker) throws Exception {
        assertTrue("Replication Main Queue has Consumer",
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        try {
                            QueueViewMBean brokerMainQueueView = getQueueView(broker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
                            return brokerMainQueueView.getConsumerCount() > 0;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return false;
                        }
                    }
                }, Wait.MAX_WAIT_MILLIS*2));
    }
}
