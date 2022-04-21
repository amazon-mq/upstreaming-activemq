package org.apache.activemq.broker.region;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.MessageAck;

public interface TopicListener {

    void onAck(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node);
}
