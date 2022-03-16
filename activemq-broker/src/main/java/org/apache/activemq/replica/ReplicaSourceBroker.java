package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueListener;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ReplicaSourceBroker extends BrokerFilter implements QueueListener {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {}; // used in destination map to indicate mirrored status
    static final String REPLICATION_CONNECTOR_NAME = "replication";

    final DestinationMap destinationsToReplicate = new DestinationMap();

    private final IdGenerator idGenerator = new IdGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    final ReplicaReplicationQueueSupplier queueProvider;
    private final URI transportConnectorUri;
    private ReplicaInternalMessageProducer replicaInternalMessageProducer;

    public ReplicaSourceBroker(Broker next, URI transportConnectorUri) {
        super(next);
        this.transportConnectorUri = Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
        replicationProducerId.setConnectionId(idGenerator.generateId());
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        TransportConnector transportConnector = next.getBrokerService().addConnector(transportConnectorUri);
        transportConnector.setName(REPLICATION_CONNECTOR_NAME);

        queueProvider.initialize();
        logger.info("Replica plugin initialized with queue {}", queueProvider.get());

        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(next, getAdminConnectionContext());

        super.start();

        ensureDestinationsAreReplicated();
    }


    private void ensureDestinationsAreReplicated() throws Exception {
        for (ActiveMQDestination d : getDurableDestinations()) { // TODO: support non-durable?
            if (shouldReplicateDestination(d)) { // TODO: specific queues?
                replicateDestinationCreation(getAdminConnectionContext(), d);
            }
        }
    }

    private boolean shouldReplicateDestination(ActiveMQDestination destination) {
        boolean isReplicationQueue = isReplicationQueue(destination);
        boolean isAdvisoryDestination = isAdvisoryDestination(destination);
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "": "not ", destination, reason);
        return shouldReplicate;
    }

    private boolean isAdvisoryDestination(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
    }

    private boolean isReplicationQueue(ActiveMQDestination destination) {
        return ReplicaSupport.REPLICATION_QUEUE_NAME.equals(destination.getPhysicalName());
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
//        return destinationsToReplicate.chooseValue(destination) != null;
        //        return destinations.stream().noneMatch(d -> d.getPhysicalName().equals(destination.getActiveMQDestination().getPhysicalName()))
    }

    private void enqueueReplicaEvent(ConnectionContext initialContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(initialContext)) {
            return;
        }

        logger.debug("Replicating {} event", event.getEventType());
        logger.error("Replicating {} event: data:\n{}", event.getEventType(), new Object() {
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
        eventMessage.setProperties(event.getReplicationProperties());
        replicaInternalMessageProducer.produceToReplicaQueue(eventMessage);
    }

    private boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

    private void replicateSend(ProducerBrokerExchange context, Message message, ActiveMQDestination destination) {
        if (message.isAdvisory()) {  // TODO: only replicate what we care about
            return;
        }

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

    private void replicateBeginTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate begin of transaction [{}]", xid);
        }
    }
    private void replicatePrepareTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction prepare [{}]", xid);
        }
    }
    private void replicateForgetTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction forget [{}]", xid);
        }
    }
    private void replicateRollbackTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction rollback [{}]", xid);
        }
    }

    private void replicateCommitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_COMMIT)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
                            .setReplicationProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY, onePhase)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate commit of transaction [{}]", xid);
        }
    }

    private void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) throws Exception {
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

    private void replicateDestinationRemoval(ConnectionContext context, ActiveMQDestination destination) {
        if (!isReplicatedDestination(destination)) {
            return;
        }
        try {
            enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                    .setEventType(ReplicaEventType.DESTINATION_DELETE)
                    .setEventData(eventSerializer.serializeReplicationData(destination))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate remove of destination {}", destination.getPhysicalName(), e);
        }
    }

    private void replicateMessageExpired(ConnectionContext context, MessageReference reference) {
        Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                        .setEventType(ReplicaEventType.MESSAGE_EXPIRED)
                        .setEventData(eventSerializer.serializeReplicationData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate discard of {}", reference.getMessageId(), e);
        }
    }

    @Override
    public Set<Destination> getDestinations(ActiveMQDestination destination) {
        return super.getDestinations(destination);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        super.acknowledge(consumerExchange, ack);
        replicateAck(consumerExchange.getConnectionContext(), consumerExchange.getSubscription(), ack);
    }

    private void replicateAck(ConnectionContext context, Subscription sub, MessageAck ack) {
        if (!isReplicatedDestination(ack.getDestination())) {
            return;
        }
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

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return super.messagePull(context, pull);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        assertAuthorized(context,  consumerInfo.getDestination(), true);

        Subscription subscription = super.addConsumer(context, consumerInfo);
        replicateAddConsumer(context, consumerInfo);
        return subscription;
    }

    private void replicateAddConsumer(ConnectionContext context, ConsumerInfo consumerInfo) {
        if (isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.ADD_CONSUMER)
                            .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
                            .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, context.getClientId())
            );
        } catch (Exception e) {
            logger.error("Failed to replicate adding {}", consumerInfo, e);
        }
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        super.removeConsumer(context, consumerInfo);
        replicateRemoveConsumer(context, consumerInfo);
    }

    private void replicateRemoveConsumer(ConnectionContext context, ConsumerInfo consumerInfo) {
        if (isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.REMOVE_CONSUMER)
                            .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
                            .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, context.getClientId())
            );
        } catch (Exception e) {
            logger.error("Failed to replicate adding {}", consumerInfo, e);
        }
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo producerInfo) throws Exception {
        // JMS allows producers to be created without first specifying a destination.  In these cases, every send
        // operation must specify a destination.  Because of this, we only authorize 'addProducer' if a destination is
        // specified. If not specified, the authz check in the 'send' method below will ensure authorization.
        if (producerInfo.getDestination() != null) {
            assertAuthorized(context, producerInfo.getDestination(), false);
        }
        super.addProducer(context, producerInfo);
    }

    private boolean isReplicationTransport(Connector connector) {
        return connector instanceof TransportConnector && ((TransportConnector) connector).getName().equals(REPLICATION_CONNECTOR_NAME);
    }

    protected void assertAuthorized(ConnectionContext context, ActiveMQDestination destination, boolean consumer) {
        boolean replicationQueue = isReplicationQueue(destination);
        boolean replicationTransport = isReplicationTransport(context.getConnector());

        if (isSystemBroker(context)) {
            return;
        }
        if (replicationTransport && consumer && (replicationQueue || isAdvisoryDestination(destination))) {
            return;
        }
        if (!replicationTransport && !replicationQueue) {
            return;
        }

        String msg = createUnauthorizedMessage(destination);
        throw new ActiveMQReplicaException(msg);
    }

    private boolean isSystemBroker(ConnectionContext context) {
        SecurityContext securityContext = context.getSecurityContext();
        return securityContext != null && securityContext.isBrokerContext();
    }

    private String createUnauthorizedMessage(ActiveMQDestination destination) {
        return "Not authorized to access destination: " + destination;
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        super.commitTransaction(context, xid, onePhase);
        replicateCommitTransaction(context, xid, onePhase);
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        super.removeSubscription(context, info); // TODO: durable subscribers?
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return super.getPreparedTransactions(context);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        int id = super.prepareTransaction(context, xid);
        replicatePrepareTransaction(context, xid);
        return id;
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.rollbackTransaction(context, xid);
        replicateRollbackTransaction(context, xid);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        super.send(producerExchange, messageSend);
        replicateSend(producerExchange, messageSend, messageSend.getDestination());
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.beginTransaction(context, xid);
        replicateBeginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        super.forgetTransaction(context, transactionId);
        replicateForgetTransaction(context, transactionId);
    }

    @Override
    public Connection[] getClients() throws Exception {
        return super.getClients();
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary)
        throws Exception {
        Destination newDestination = super.addDestination(context, destination, createIfTemporary);
        if (shouldReplicateDestination(destination)) {
            replicateDestinationCreation(context, destination);
            if (destination.isQueue()) {
                Queue queue = QueueExtractor.extractQueue(newDestination);
                if (queue != null && !queue.getListeners().contains(this)) {
                    queue.addListener(this);
                }
            }
        }
        return newDestination;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        replicateDestinationRemoval(context, destination);
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        return super.getDestinations();
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        return super.getPeerBrokerInfos();
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        super.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        super.postProcessDispatch(messageDispatch);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        super.processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return super.getDurableDestinations();
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        super.addDestinationInfo(context, info);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        super.removeDestinationInfo(context, info);
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        super.messageExpired(context, message, subscription);
        replicateMessageExpired(context, message);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription,
                                         Throwable poisonCause) {
        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        super.messageDelivered(context, messageReference);
    }

    @Override
    public void virtualDestinationAdded(ConnectionContext context, VirtualDestination virtualDestination) {
        super.virtualDestinationAdded(context, virtualDestination);
    }

    @Override
    public void virtualDestinationRemoved(ConnectionContext context, VirtualDestination virtualDestination) {
        super.virtualDestinationRemoved(context, virtualDestination);
    }

    @Override
    public void onDropMessage(List<MessageReference> messageReferences) {
        if (messageReferences.isEmpty()) {
            return;
        }

        ActiveMQDestination destination = messageReferences.get(0).getMessage().getDestination();
        if (!isReplicatedDestination(destination)) {
            return;
        }
        final List<String> messageIds = messageReferences.stream()
                .map(MessageReference::getMessageId)
                .map(MessageId::toString)
                .collect(Collectors.toList());

        try {
            enqueueReplicaEvent(
                    null,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGES_DROPPED)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
                            .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIds)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate drop messages {}", destination, e);
        }
    }
}
