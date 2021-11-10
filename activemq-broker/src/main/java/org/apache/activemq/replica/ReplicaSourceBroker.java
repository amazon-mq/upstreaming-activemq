package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaSourceBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);

    public ReplicaSourceBroker(final Broker next) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
        logger.info("Replica plugin initialized");
    }
}
