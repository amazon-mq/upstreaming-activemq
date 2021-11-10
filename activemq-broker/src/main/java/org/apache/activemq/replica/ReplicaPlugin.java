package org.apache.activemq.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * A Broker plugin to replicate core messaging events from one broker to another.
 *
 * @org.apache.xbean.XBean element="replicaPlugin"
 */
public class ReplicaPlugin extends BrokerPluginSupport {

    private final Logger logger = LoggerFactory.getLogger(ReplicaPlugin.class);

    protected ReplicaRole role = ReplicaRole.source;
    protected ActiveMQConnectionFactory otherBrokerConnectionFactory = null;

    public ReplicaPlugin() {
        super();
    }

    @Override
    public Broker installPlugin(final Broker broker) {
        logger.info("{} installed, running as {}", ReplicaPlugin.class.getName(), role);
        return role == ReplicaRole.replica
            ? new ReplicaBroker(broker, otherBrokerConnectionFactory)
            : new ReplicaSourceBroker(broker);
    }

    public ReplicaPlugin setRole(ReplicaRole role) {
        this.role = requireNonNull(role);
        return this;
    }

    public ReplicaPlugin connectedTo(URI uri) {
        this.setOtherBrokerUri(requireNonNull(uri).toString());
        return this;
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setRole(String role) {
        this.role = Arrays.stream(ReplicaRole.values())
            .filter(roleValue -> roleValue.name().equalsIgnoreCase(role))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(role + " is not a known " + ReplicaRole.class.getSimpleName()));
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setOtherBrokerUri(String uri) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(uri); // once to validate
        connectionFactory.setBrokerURL(
            uri.toLowerCase().startsWith("failover:(")
                ? uri
                : "failover:("+uri+")"
        );
        this.otherBrokerConnectionFactory = connectionFactory;
    }

    public ReplicaRole getRole() {
        return role;
    }
}
