package org.apache.activemq.replica;

import static java.util.Objects.requireNonNull;

import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.activemq.broker.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaBrokerEventListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final Broker broker;

    ReplicaBrokerEventListener(final Broker broker) {
        this.broker = requireNonNull(broker);
    }

    @Override
    public void onMessage(final Message jmsMessage) {
        logger.trace("Received replication message from replica source");
    }

}
