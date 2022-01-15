package org.apache.activemq.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.net.URI;
import java.util.Arrays;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

public class ReplicaPluginTest {

    private final ReplicaPlugin plugin = new ReplicaPlugin();

    @Test
    public void canSetRole() {
        SoftAssertions softly = new SoftAssertions();
        Arrays.stream(ReplicaRole.values()).forEach(role -> {

            softly.assertThat(plugin.setRole(role)).isSameAs(plugin);
            softly.assertThat(plugin.getRole()).isEqualTo(role);

            plugin.setRole(role.name());
            softly.assertThat(plugin.getRole()).isEqualTo(role);
        });
        softly.assertAll();
    }

    @Test
    public void rejectsUnknownRole() {
        Throwable exception = catchThrowable(() -> plugin.setRole("unknown"));

        assertThat(exception).isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("unknown is not a known " + ReplicaRole.class.getSimpleName());
    }

    @Test
    public void canSetOtherBrokerUri() {
        plugin.setOtherBrokerUri("failover:(tcp://localhost:61616)");

        assertThat(plugin.otherBrokerConnectionFactory).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void canSetOtherBrokerUriFluently() {
        ReplicaPlugin result = plugin.connectedTo(URI.create("failover:(tcp://localhost:61616)"));

        assertThat(result).isSameAs(plugin);
        assertThat(result.otherBrokerConnectionFactory).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void rejectsInvalidUnknownOtherBrokerUri() {
        Throwable expected = catchThrowable(() -> new ActiveMQConnectionFactory().setBrokerURL("inval:{id}-uri"));

        Throwable exception = catchThrowable(() -> plugin.setOtherBrokerUri("inval:{id}-uri"));

        assertThat(exception).isNotNull().isEqualToComparingFieldByField(expected);
    }

    @Test
    public void canSetOtherBrokerUriWithAutomaticAdditionOfFailoverTransport() {
        plugin.setOtherBrokerUri("tcp://localhost:61616");

        assertThat(plugin.otherBrokerConnectionFactory).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void canSetTransportConnectorUri() {
        plugin.setTransportConnectorUri("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600");

        assertThat(plugin.transportConnectorUri).isNotNull()
                .isEqualTo(URI.create("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600"));
    }

    @Test
    public void rejectsInvalidTransportConnectorUri() {
        Throwable expected = catchThrowable(() -> URI.create("inval:{id}-uri"));

        Throwable exception = catchThrowable(() -> plugin.setTransportConnectorUri("inval:{id}-uri"));

        assertThat(exception).isNotNull().isEqualToComparingFieldByField(expected);
    }

}
