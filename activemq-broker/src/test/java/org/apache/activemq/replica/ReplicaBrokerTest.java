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
