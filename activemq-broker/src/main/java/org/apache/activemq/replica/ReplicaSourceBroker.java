package org.apache.activemq.replica;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaSourceBroker extends BrokerFilter {

    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<Boolean>() {}; // used in destination map to indicate mirrored status

    final DestinationMap destinationsToReplicate = new DestinationMap();

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private final IdGenerator idGenerator = new IdGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ReplicaReplicationQueueSupplier queueProvider;

    public ReplicaSourceBroker(final Broker next) {
        super(next);
        replicationProducerId.setConnectionId(idGenerator.generateId());
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        super.start();
        queueProvider.initialize();
        logger.info("Replica plugin initialized with queue {}", queueProvider.get());
        ensureDestinationsAreReplicated();
        addReplicationInterceptor();
    }


    private void ensureDestinationsAreReplicated() throws Exception {
        for (ActiveMQDestination d : getDurableDestinations()) { // TODO: support non-durable?
            if (shouldReplicateDestination(d)) { // TODO: specific queues?
                replicateDestinationCreation(getAdminConnectionContext(), d);
            }
        }
    }

    private boolean shouldReplicateDestination(final ActiveMQDestination destination) {
        boolean isReplicationQueue = destination.getPhysicalName().startsWith(ReplicaSupport.REPLICATION_QUEUE_PREFIX);
        boolean isAdvisoryDestination = destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "": "not ", destination, reason);
        return shouldReplicate;
    }

    public boolean isReplicatedDestination(final ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
//        return destinationsToReplicate.chooseValue(destination) != null;
        //        return destinations.stream().noneMatch(d -> d.getPhysicalName().equals(destination.getActiveMQDestination().getPhysicalName()))
    }

    private void enqueueReplicaEvent(ConnectionContext context, ReplicaEvent event) throws Exception {
        logger.debug("Replicating {} event", event.getEventType());
        logger.trace("data:\n{}", new Object() {
            @Override
            public String toString() {
                try {
                    return eventSerializer.deserializeMessageData(event.getEventData()).toString();
                } catch (IOException e) {
                    return "<some event data>";
                }
            }
        }); // FIXME: remove
        ActiveMQMessage eventMessage = new ActiveMQMessage();
        eventMessage.setPersistent(true);
        eventMessage.setType("ReplicaEvent");
        eventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        eventMessage.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
        eventMessage.setDestination(queueProvider.get());
        eventMessage.setProducerId(replicationProducerId);
        eventMessage.setResponseRequired(false);
        eventMessage.setContent(event.getEventData());
        new ReplicaInternalMessageProducer(next, context).produceToReplicaQueue(eventMessage);
    }

    private void addReplicationInterceptor() {
        BrokerService brokerService = getBrokerService();
        brokerService.setDestinationInterceptors(
            Stream.concat(
                Arrays.stream(brokerService.getDestinationInterceptors()),
                Stream.of(new ReplicationDestinationInterceptor(this))
            ).toArray(DestinationInterceptor[]::new)
        );
    }

    private void replicateDestinationCreation(final ConnectionContext context, final ActiveMQDestination destination) throws Exception {
        enqueueReplicaEvent(
            context,
            new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                .setEventData(eventSerializer.serializeReplicationData(destination))
        );
        if (destinationsToReplicate.chooseValue(destination) == null) {
            destinationsToReplicate.put(destination, IS_REPLICATED);
        }
    }

    static class ReplicationDestinationInterceptor implements DestinationInterceptor {

        private final ReplicaSourceBroker replicaSourceBroker;

        ReplicationDestinationInterceptor(final ReplicaSourceBroker replicaSourceBroker) {
            this.replicaSourceBroker = replicaSourceBroker;
        }

        @Override
        public Destination intercept(final Destination destination) {
            if (!replicaSourceBroker.isReplicatedDestination(destination.getActiveMQDestination())) {
                return destination;
            }
            return new DestinationFilter(destination) {

                @Override
                protected void send(final ProducerBrokerExchange context, final Message message, final ActiveMQDestination destination)
                    throws Exception {
                    super.send(context, message, destination);
                }

                @Override
                public void acknowledge(final ConnectionContext context, final Subscription sub, final MessageAck ack,
                                        final MessageReference node) throws IOException {
                    super.acknowledge(context, sub, ack, node);
                }
            };
        }

        @Override
        public void remove(final Destination destination) {
        }

        @Override
        public void create(final Broker broker, final ConnectionContext context, final ActiveMQDestination destination) throws Exception {
        }
    }
}
