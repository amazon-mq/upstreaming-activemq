package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

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

    private void replicateSend(final ProducerBrokerExchange context, final Message message,
                               final ActiveMQDestination destination) {
        try {
            enqueueReplicaEvent(
                context.getConnectionContext(),
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.MESSAGE_SEND)
                    .setEventData(eventSerializer.serializeMessageData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate message {} for destination {}", message.getMessageId(), destination.getPhysicalName());
        }
    }

    private void replicateAck(final ConnectionContext context, final Subscription sub, final MessageAck ack) {
        try {
            enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.MESSAGE_ACK)
                    .setEventData(eventSerializer.serializeReplicationData(ack))
            );
        } catch (Exception e) {
            logger.error(
                "Failed to replicate ACK {}<->{} for consumer {}",
                ack.getFirstMessageId(),
                ack.getLastMessageId(),
                sub.getConsumerInfo()
            );
        }
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

    private void replicateDestinationRemoval(final ActiveMQDestination destination) {
        if (!isReplicatedDestination(destination)) {
            return;
        }
        try {
            enqueueReplicaEvent(
                getAdminConnectionContext(),
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.DESTINATION_DELETE)
                    .setEventData(eventSerializer.serializeReplicationData(destination))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate remove of destination {}", destination.getPhysicalName(), e);
        }
    }

    private void replicateMessageConsumed(ConnectionContext context, MessageReference reference) {
        final Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }
        try {
            final MessageAck ackToReplicate = createAckFromReference(reference, null);
            enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.MESSAGE_CONSUMED)
                    .setEventData(eventSerializer.serializeReplicationData(ackToReplicate))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate consumption {}", reference.getMessageId(), e);
        }
    }

    private void replicateMessageDiscarded(ConnectionContext context, MessageReference reference) {
        final Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }
        try {
            final MessageAck ackToReplicate = createAckFromReference(reference, null);
            enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.MESSAGE_DISCARDED)
                    .setEventData(eventSerializer.serializeReplicationData(ackToReplicate))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate discard of {}", reference.getMessageId(), e);
        }
    }

    private void replicateMessageExpired(ConnectionContext context, MessageReference reference) {
        final Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }
        try {
            final MessageAck ackToReplicate = createAckFromReference(reference, null);
            enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.MESSAGE_EXPIRED)
                    .setEventData(eventSerializer.serializeReplicationData(ackToReplicate))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate discard of {}", reference.getMessageId(), e);
        }
    }

    private MessageAck createAckFromReference(final MessageReference reference, final ConsumerId consumerId) {
        final MessageAck ack = new MessageAck(reference.getMessage(), MessageAck.INDIVIDUAL_ACK_TYPE, 1);
        ack.setConsumerId(consumerId);
        return ack;
    }

//
//    @Override
//    public void stop() throws Exception {
//        super.stop();
//    }

    //    @Override
//    public Broker getAdaptor(final Class<?> type) {
//        return super.getAdaptor(type);
//    }

//    @Override
//    public Broker getNext() {
//        return super.getNext();
//    }
//
//    @Override
//    public void setNext(final Broker next) {
//        super.setNext(next);
//    }

//    @Override
//    public Map<ActiveMQDestination, Destination> getDestinationMap() {
//        return super.getDestinationMap();
//    }
//
//    @Override
//    public Map<ActiveMQDestination, Destination> getDestinationMap(final ActiveMQDestination destination) {
//        return super.getDestinationMap(destination);
//    }

    @Override
    public Set<Destination> getDestinations(final ActiveMQDestination destination) {
        return super.getDestinations(destination);
    }

    @Override // it seems like this acknowledge is a client->server connection where the broker server is telling the client, ACK, got it
    public void acknowledge(final ConsumerBrokerExchange consumerExchange, final MessageAck ack) throws Exception {
        super.acknowledge(consumerExchange, ack);
//        if (consumerExchange.getSubscription().isBrowser() || !isReplicatedDestination(ack.getDestination())) {
//            return;
//        }
//        replicateAck(consumerExchange.getConnectionContext(), consumerExchange.getSubscription(), ack);
    }

    @Override
    public Response messagePull(final ConnectionContext context, final MessagePull pull) throws Exception {
        return super.messagePull(context, pull);
    }

//    @Override
//    public void addConnection(final ConnectionContext context, final ConnectionInfo info) throws Exception {
//        super.addConnection(context, info);
//    }

    @Override
    public Subscription addConsumer(final ConnectionContext context, final ConsumerInfo consumerInfo) throws Exception {
        Subscription subscription = super.addConsumer(context, consumerInfo);
        SubscriptionInfo subscriptionInfo = new SubscriptionInfo(
            subscription.getConsumerInfo().getClientId(),
            subscription.getConsumerInfo().getSubscriptionName()
        );
        subscriptionInfo.setSelector(subscription.getSelector());
        subscriptionInfo.setDestination(subscriptionInfo.getDestination()); // TODO: durable subscribers?
        return subscription;
    }

//    @Override
//    public void addProducer(final ConnectionContext context, final ProducerInfo info) throws Exception {
//        super.addProducer(context, info);
//    }

    @Override
    public void commitTransaction(final ConnectionContext context, final TransactionId xid, final boolean onePhase) throws Exception {
        logger.warn("Should commit transaction {}", xid);
        super.commitTransaction(context, xid, onePhase);
    }

    @Override
    public void removeSubscription(final ConnectionContext context, final RemoveSubscriptionInfo info) throws Exception {
        super.removeSubscription(context, info); // TODO: durable subscribers?
    }

    @Override
    public TransactionId[] getPreparedTransactions(final ConnectionContext context) throws Exception {
        return super.getPreparedTransactions(context);
    }

    @Override
    public int prepareTransaction(final ConnectionContext context, final TransactionId xid) throws Exception {
        return super.prepareTransaction(context, xid);
    }

    @Override
    public void rollbackTransaction(final ConnectionContext context, final TransactionId xid) throws Exception {
        super.rollbackTransaction(context, xid);
    }

    @Override
    public void send(final ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        super.send(producerExchange, messageSend);
        replicateSend(producerExchange, messageSend, messageSend.getDestination()); // TODO: only replicate what we care about
    }

    @Override
    public void beginTransaction(final ConnectionContext context, final TransactionId xid) throws Exception {
        super.beginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(final ConnectionContext context, final TransactionId transactionId) throws Exception {
        super.forgetTransaction(context, transactionId);
    }

    @Override
    public Connection[] getClients() throws Exception {
        return super.getClients();
    }

    @Override
    public Destination addDestination(final ConnectionContext context, final ActiveMQDestination destination, final boolean createIfTemporary)
        throws Exception {
        Destination newDestination = super.addDestination(context, destination, createIfTemporary);
        if (shouldReplicateDestination(destination)) {
            replicateDestinationCreation(context, destination);
        }
        return newDestination;
    }

    @Override
    public void removeDestination(final ConnectionContext context, final ActiveMQDestination destination, final long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        replicateDestinationRemoval(destination);
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        return super.getDestinations();
    }
//
//    @Override
//    public void addSession(final ConnectionContext context, final SessionInfo info) throws Exception {
//        super.addSession(context, info);
//    }
//
//    @Override
//    public void removeSession(final ConnectionContext context, final SessionInfo info) throws Exception {
//        super.removeSession(context, info);
//    }
//
//    @Override
//    public BrokerId getBrokerId() {
//        return super.getBrokerId();
//    }
//
//    @Override
//    public String getBrokerName() {
//        return super.getBrokerName();
//    }
//
//    @Override
//    public void gc() {
//        super.gc();
//    }
//
//    @Override
//    public void addBroker(final Connection connection, final BrokerInfo info) {
//        super.addBroker(connection, info);
//    }
//
//    @Override
//    public void removeBroker(final Connection connection, final BrokerInfo info) {
//        super.removeBroker(connection, info);
//    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        return super.getPeerBrokerInfos();
    }

    @Override
    public void preProcessDispatch(final MessageDispatch messageDispatch) {
        super.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(final MessageDispatch messageDispatch) {
        super.postProcessDispatch(messageDispatch);
    }

    @Override
    public void processDispatchNotification(final MessageDispatchNotification messageDispatchNotification) throws Exception {
        super.processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return super.getDurableDestinations();
    }

    @Override
    public void addDestinationInfo(final ConnectionContext context, final DestinationInfo info) throws Exception {
        super.addDestinationInfo(context, info);
    }

    @Override
    public void removeDestinationInfo(final ConnectionContext context, final DestinationInfo info) throws Exception {
        super.removeDestinationInfo(context, info);
    }

    @Override
    public void messageExpired(final ConnectionContext context, final MessageReference message, final Subscription subscription) {
        super.messageExpired(context, message, subscription);
        replicateMessageExpired(context, message);
    }

    @Override
    public boolean sendToDeadLetterQueue(final ConnectionContext context, final MessageReference messageReference, final Subscription subscription,
                                         final Throwable poisonCause) {
        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public void messageConsumed(final ConnectionContext context, final MessageReference messageReference) {
        super.messageConsumed(context, messageReference);
        replicateMessageConsumed(context, messageReference);
    }

    @Override
    public void messageDelivered(final ConnectionContext context, final MessageReference messageReference) {
        super.messageDelivered(context, messageReference);
    }

    @Override
    public void messageDiscarded(final ConnectionContext context, final Subscription sub, final MessageReference messageReference) {
        super.messageDiscarded(context, sub, messageReference);
        replicateMessageDiscarded(context, messageReference);
    }

    @Override
    public void virtualDestinationAdded(final ConnectionContext context, final VirtualDestination virtualDestination) {
        super.virtualDestinationAdded(context, virtualDestination);
    }

    @Override
    public void virtualDestinationRemoved(final ConnectionContext context, final VirtualDestination virtualDestination) {
        super.virtualDestinationRemoved(context, virtualDestination);
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
                    replicaSourceBroker.replicateSend(context, message, destination);
                }

                @Override
                public void acknowledge(final ConnectionContext context, final Subscription sub, final MessageAck ack,
                                        final MessageReference node) throws IOException {
                    super.acknowledge(context, sub, ack, node);
                    replicaSourceBroker.replicateAck(context, sub, ack);
                }
            };
        }

        @Override
        public void remove(final Destination destination) {
            replicaSourceBroker.replicateDestinationRemoval(destination.getActiveMQDestination());
        }

        @Override
        public void create(final Broker broker, final ConnectionContext context, final ActiveMQDestination destination) throws Exception {
            if (replicaSourceBroker.shouldReplicateDestination(destination)) {
                replicaSourceBroker.replicateDestinationCreation(context, destination);
            }
        }
    }
}
