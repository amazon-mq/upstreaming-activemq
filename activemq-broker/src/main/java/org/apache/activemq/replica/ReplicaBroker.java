package org.apache.activemq.replica;

import static java.util.Objects.requireNonNull;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBroker.class);
    private final ActiveMQConnectionFactory replicaSourceConnectionFactory;

    public ReplicaBroker(final Broker next, final ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        super(next);
        this.replicaSourceConnectionFactory = requireNonNull(replicaSourceConnectionFactory, "Need connection details of replica source for this broker");
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

}
