package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.util.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ReplicaBrokerSubscriptionHandler subscriptionHandler;
    private final Broker broker;

    ReplicaBrokerEventListener(final Broker broker) {
        this(broker, new ReplicaBrokerSubscriptionHandler(broker));
    }

    ReplicaBrokerEventListener(final Broker broker, final ReplicaBrokerSubscriptionHandler subscriptionHandler) {
        this.broker = requireNonNull(broker);
        this.subscriptionHandler = requireNonNull(subscriptionHandler);
    }

    @Override
    public void onMessage(final Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        ActiveMQMessage message = (ActiveMQMessage) jmsMessage;
        ByteSequence messageContent = message.getContent();

        try {
            Object deserializedData = eventSerializer.deserializeMessageData(messageContent);
            logger.trace(deserializedData.toString());
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
                    case MESSAGE_CONSUMED: // TODO: make sure advisory correctly fired
                    case MESSAGE_EXPIRED:
                    case MESSAGE_DISCARDED:
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
                    default:
                        logger.warn("Unhandled event type \"{}\" for replication message id: {}", eventType, message.getJMSMessageID());
                }
            });
        } catch (IOException | ClassCastException e) {
            logger.error("Failed to deserialize replication message (id={}), {}", message.getMessageId(), new String(messageContent.data));
            logger.debug("Deserialization error for replication message (id={})", message.getMessageId(), e);
        }
    }

    private Optional<ReplicaEventType> getEventType(final ActiveMQMessage message) {
        try {
            String eventTypeProperty = message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY);
            return Arrays.stream(ReplicaEventType.values())
                .filter(t -> t.name().equals(eventTypeProperty))
                .findFirst();
        }
        catch (JMSException e) {
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
            Destination destination = broker.getDestinations(ack.getDestination()).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Destination not found that matches: " + ack.getDestination().getQualifiedName()));
            consumerBrokerExchange.setRegion(broker);
            consumerBrokerExchange.setRegionDestination(destination);
            consumerBrokerExchange.setConnectionContext(broker.getAdminConnectionContext());
            final ConsumerId newOrExistingConsumerId = subscriptionHandler.createSubscriptionIfAbsent(
                ack.getConsumerId(),
                ack.getDestination()
            );
            ack.setConsumerId(newOrExistingConsumerId);
            RegionBroker regionBroker = (RegionBroker) broker.getAdaptor(RegionBroker.class);
            Region region = regionBroker.getRegion(destination.getActiveMQDestination());
            region.acknowledge(consumerBrokerExchange, ack);
        } catch (Exception e) {
            logger.error("Failed to process ack with last message id: {}", ack.getLastMessageId(), e);
        }
    }

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

    private void consumeMessage(final MessageReference messageReference) {
        broker.getRoot().messageConsumed(broker.getAdminConnectionContext(), messageReference);
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

}
