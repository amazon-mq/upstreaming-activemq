package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final Broker broker;

    ReplicaBrokerEventListener(final Broker broker) {
        this.broker = requireNonNull(broker);
    }

    @Override
    public void onMessage(final Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        ActiveMQMessage message = (ActiveMQMessage) jmsMessage;
        ByteSequence messageContent = message.getContent();

        try {
            Object deserializedData = eventSerializer.deserializeMessageData(messageContent);
            getEventType(message).ifPresent(eventType -> {
                switch (eventType) {
                    case MESSAGE_SEND:
                        logger.trace("Processing replicated message send");
                        persistMessage((ActiveMQMessage) deserializedData);
                        return;
                    case MESSAGE_ACK:
                        logger.trace("Processing replicated message ack");
                        consumeAck((MessageAck) deserializedData);
                        return;
                    case MESSAGE_EXPIRED:
                    case MESSAGE_DISCARDED:
                    case MESSAGE_DROPPED:
                        logger.trace("Processing replicated message removal due to {}", eventType);
                        removeMessage((MessageAck) deserializedData);
                        return;
                    case DESTINATION_UPSERT:
                        logger.trace("Processing replicated destination");
                        upsertDestination((ActiveMQDestination) deserializedData);
                        return;
                    case DESTINATION_DELETE:
                        logger.trace("Processing replicated destination deletion");
                        deleteDestination((ActiveMQDestination) deserializedData);
                        return;
                    case TRANSACTION_BEGIN:
                        logger.trace("Processing replicated transaction begin");
                        beginTransaction((TransactionId) deserializedData);
                        return;
                    case TRANSACTION_PREPARE:
                        logger.trace("Processing replicated transaction prepare");
                        prepareTransaction((TransactionId) deserializedData);
                        return;
                    case TRANSACTION_FORGET:
                        logger.trace("Processing replicated transaction forget");
                        forgetTransaction((TransactionId) deserializedData);
                        return;
                    case TRANSACTION_ROLLBACK:
                        logger.trace("Processing replicated transaction rollback");
                        rollbackTransaction((TransactionId) deserializedData);
                        return;
                    case TRANSACTION_COMMIT:
                        logger.trace("Processing replicated transaction commit");
                        try {
                            commitTransaction(
                                    (TransactionId) deserializedData,
                                    message.getBooleanProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY));
                        } catch (JMSException e) {
                            logger.error("Failed to extract property to replicate transaction commit with id [{}]", deserializedData, e);
                        }
                        return;
                    case ADD_CONSUMER:
                        logger.trace("Processing replicated add consumer");
                        try {
                            addConsumer((ConsumerInfo) deserializedData,
                                    message.getStringProperty(ReplicaSupport.CLIENT_ID_PROPERTY));
                        } catch (JMSException e) {
                            logger.error("Failed to extract property to replicate add consumer [{}]", deserializedData, e);
                        }
                        return;
                    case REMOVE_CONSUMER:
                        logger.trace("Processing replicated remove consumer");
                        try {
                            removeConsumer((ConsumerInfo) deserializedData,
                                    message.getStringProperty(ReplicaSupport.CLIENT_ID_PROPERTY));
                        } catch (JMSException e) {
                            logger.error("Failed to extract property to replicate remove consumer [{}]", deserializedData, e);
                        }
                        return;
                    default:
                        logger.warn("Unhandled event type \"{}\" for replication message id: {}", eventType, message.getJMSMessageID());
                }
            });
            message.acknowledge();
        } catch (IOException | ClassCastException e) {
            logger.error("Failed to deserialize replication message (id={}), {}", message.getMessageId(), new String(messageContent.data));
            logger.debug("Deserialization error for replication message (id={})", message.getMessageId(), e);
        } catch (JMSException e) {
            logger.error("Failed to acknowledge replication message (id={})", message.getMessageId());
        }
    }

    private Optional<ReplicaEventType> getEventType(final ActiveMQMessage message) {
        try {
            String eventTypeProperty = message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY);
            return Arrays.stream(ReplicaEventType.values())
                    .filter(t -> t.name().equals(eventTypeProperty))
                    .findFirst();
        } catch (JMSException e) {
            logger.error("Failed to get {} property {}", ReplicaEventType.class.getSimpleName(), ReplicaEventType.EVENT_TYPE_PROPERTY, e);
            return Optional.empty();
        }
    }

    private void persistMessage(final ActiveMQMessage message) {
        try {
            new ReplicaInternalMessageProducer(broker).produceToReplicaQueue(message);
        } catch (Exception e) {
            logger.error("Failed to process message {} with JMS message id: {}", message.getMessageId(), message.getJMSMessageID(), e);
        }
    }

    private void consumeAck(final MessageAck ack) {
        try {
            ConsumerBrokerExchange consumerBrokerExchange = new ConsumerBrokerExchange();
            consumerBrokerExchange.setConnectionContext(broker.getAdminConnectionContext());
            broker.acknowledge(consumerBrokerExchange, ack);
        } catch (Exception e) {
            logger.error("Failed to process ack with last message id: {}", ack.getLastMessageId(), e);
        }
    }

    // TODO get rid of the method
    private void removeMessage(final MessageAck messageAck) {
        for (Destination destination : broker.getDestinations(messageAck.getDestination())) {
            try {
                if (destination instanceof Queue) {
                    ((Queue) destination).removeMessage(messageAck.getLastMessageId().toString());
                } else if (destination instanceof Topic) {
                    handleRemoveForTopic((Topic) destination, messageAck);
                } else {
                    logger.error("Unhandled destination type {} for ack {}", destination.getClass(), messageAck);
                }
            } catch (Exception e) {
                logger.error("Failed to process removal for message ack {}", messageAck);
            }
        }
    }

    private void handleRemoveForTopic(final Topic topic, final MessageAck messageAck) throws IOException {
        Optional<Subscription> subscriptionForWhichThisAckIsReplicated = Optional.ofNullable(broker.getAdaptor(AbstractRegion.class))
                .map(AbstractRegion.class::cast)
                .map(AbstractRegion::getSubscriptions)
                .map(subscriptions -> subscriptions.get(messageAck.getConsumerId()))
                .filter(DurableTopicSubscription.class::isInstance);

        if (subscriptionForWhichThisAckIsReplicated.isPresent()) {
            org.apache.activemq.command.Message message = topic.loadMessage(messageAck.getFirstMessageId()); // TODO: think about efficiency of this and if we can just ack without a full message retrieval
            topic.acknowledge(
                    broker.getAdminConnectionContext(),
                    subscriptionForWhichThisAckIsReplicated.get(),
                    messageAck,
                    new IndirectMessageReference(message)
            );
        }
    }

    private void upsertDestination(final ActiveMQDestination destination) {
        try {
            boolean isExistingDestination = Arrays.stream(broker.getDestinations())
                    .anyMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isExistingDestination) {
                logger.debug("Destination [{}] already exists, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
        }
        try {
            broker.addDestination(broker.getAdminConnectionContext(), destination, true);
        } catch (Exception e) {
            logger.error("Unable to add destination [{}]", destination, e);
        }
    }

    private void deleteDestination(final ActiveMQDestination destination) {
        try {
            boolean isNonExtantDestination = Arrays.stream(broker.getDestinations())
                    .noneMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isNonExtantDestination) {
                logger.debug("Destination [{}] does not exist, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
        }
        try {
            broker.removeDestination(broker.getAdminConnectionContext(), destination, 1000);
        } catch (Exception e) {
            logger.error("Unable to remove destination [{}]", destination, e);
        }
    }

    private void beginTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.beginTransaction(broker.getAdminConnectionContext(), xid);
        } catch (Exception e) {
            logger.error("Unable to replicate begin transaction [{}]", xid, e);
        }
    }

    private void prepareTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.prepareTransaction(broker.getAdminConnectionContext(), xid);
        } catch (Exception e) {
            logger.error("Unable to replicate prepare transaction [{}]", xid, e);
        }
    }

    private void forgetTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.forgetTransaction(broker.getAdminConnectionContext(), xid);
        } catch (Exception e) {
            logger.error("Unable to replicate forget transaction [{}]", xid, e);
        }
    }

    private void rollbackTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.rollbackTransaction(broker.getAdminConnectionContext(), xid);
        } catch (Exception e) {
            logger.error("Unable to replicate rollback transaction [{}]", xid, e);
        }
    }

    private void commitTransaction(TransactionId xid, boolean onePhase) {
        try {
            broker.commitTransaction(broker.getAdminConnectionContext(), xid, onePhase);
        } catch (Exception e) {
            logger.error("Unable to replicate commit transaction [{}]", xid, e);
        }
    }

    private void addConsumer(ConsumerInfo consumerInfo, String clientId) {
        try {
            ConnectionContext context = broker.getAdminConnectionContext().copy();
            context.setClientId(clientId);
            context.setConnection(new DummyConnection());
            broker.addConsumer(context, consumerInfo);
        } catch (Exception e) {
            logger.error("Unable to replicate add consumer [{}]", consumerInfo, e);
        }
    }

    private void removeConsumer(ConsumerInfo consumerInfo, String clientId) {
        try {
            ConnectionContext context = broker.getAdminConnectionContext().copy();
            context.setClientId(clientId);
            broker.removeConsumer(context, consumerInfo);
        } catch (Exception e) {
            logger.error("Unable to replicate remove consumer [{}]", consumerInfo, e);
        }
    }

    private void createTransactionMapIfNotExist() {
        if (broker.getAdminConnectionContext().getTransactions() == null) {
            broker.getAdminConnectionContext().setTransactions(new ConcurrentHashMap<>());
        }
    }

}
