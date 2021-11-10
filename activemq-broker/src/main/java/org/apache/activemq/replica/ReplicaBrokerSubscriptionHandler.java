package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.text.MessageFormat.format;
import static java.util.Objects.requireNonNull;

public class ReplicaBrokerSubscriptionHandler {

    /**
     * We require a subscription to exist to ack a message. So local subscriptions must be made, and use the same consumerId
     * as the replica source, but then should be expired periodically as they won't be removed like they would in the source
     * broker.
     */
    private static final Duration REPLICA_SUBSCRIBER_TTL = Duration.ofMinutes(5L);

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerSubscriptionHandler.class);
    private final AtomicLong subscriptionHandlerThreadId = new AtomicLong(0);
    private final Map<ConsumerId, Instant> knownConsumers = new ConcurrentHashMap<>();
    private final IdGenerator connectionIdGenerator = new IdGenerator("replica");
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final ScheduledExecutorService subscriptionHandler;
    private final Broker broker;
    private final ConnectionContext replicaBrokerConnectionContext;

    public ReplicaBrokerSubscriptionHandler(final Broker broker) {
        this.subscriptionHandler = Executors.newScheduledThreadPool(
                0,
                runnable -> {
                    Thread thread = new Thread(
                        runnable,
                        format("{0}-{1}", getClass().getSimpleName(), subscriptionHandlerThreadId.incrementAndGet())
                    );
                    thread.setDaemon(true);
                    thread.setUncaughtExceptionHandler((t, e) -> logger.error("Error in thread '{}'", t.getName(), e));
                    return thread;
                }
            );
        this.broker = broker;
        this.replicaBrokerConnectionContext = broker.getAdminConnectionContext().copy();
        replicaBrokerConnectionContext.setClientId("replica-internal-context");
    }

    // TODO: durable subscribers replication
    void createDurableSubscription(final String clientId, final String subscriptionName) throws Exception {

    }

    ConsumerId createSubscriptionIfAbsent(final ConsumerId incomingConsumerId, final ActiveMQDestination destination) throws Exception {
        if (incomingConsumerId != null && knownConsumers.containsKey(incomingConsumerId)) {
            logger.trace("Consumer {} already exists for destination {}", incomingConsumerId, destination);
            return incomingConsumerId;
        }
        ConsumerId consumerId = incomingConsumerId;
        RegionBroker regionBroker = (RegionBroker) broker.getAdaptor(RegionBroker.class);
        Region region = regionBroker.getRegion(destination);
        Subscription subscription = null;
        if (consumerId != null && region instanceof AbstractRegion) {
            subscription = ((AbstractRegion) region).getSubscriptions().get(consumerId);
            if (subscription != null) {
                logger.debug("Will reuse an existing consumer subscription {} for destination {}", consumerId, destination);
                knownConsumers.put(consumerId, Instant.now());
            }
        }
        if (consumerId == null) {
            ConnectionId connectionId = new ConnectionId(connectionIdGenerator.generateId());
            SessionId sessionId = new SessionId(connectionId, sessionIdGenerator.getNextSequenceId());
            consumerId = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
        }
        if (knownConsumers.get(consumerId) == null) {
            ConsumerInfo consumerInfo = new ConsumerInfo(consumerId);
            consumerInfo.setDestination(destination);
            consumerInfo.setPrefetchSize(1);
            logger.debug("Creating consumer {} on destination {}", consumerId, destination);
//            consumerInfo.setSubscriptionName("consumer-for-"+consumerId);
            subscription = broker.addConsumer(
                replicaBrokerConnectionContext,
                consumerInfo
            );
            knownConsumers.put(consumerId, Instant.now());
        }
        final ConsumerInfo consumerInfo = requireNonNull(subscription).getConsumerInfo();
        if (!consumerInfo.isDurable()) {
            subscriptionHandler.schedule(
                () -> knownConsumers.computeIfPresent(consumerInfo.getConsumerId(), (id, timeSubscriptionShouldExpire) -> {
                    final Instant oldestAllowableSubscriptionTime = Instant.now().minus(REPLICA_SUBSCRIBER_TTL);
                    if (timeSubscriptionShouldExpire.isAfter(oldestAllowableSubscriptionTime)) {
                        try {
                            logger.debug("Removing consumer {} on destination {}", consumerInfo, destination);
                            broker.removeConsumer(replicaBrokerConnectionContext, consumerInfo);
                        } catch (Exception e) {
                            logger.error("Failed to expire consumer {} on destination {}", consumerInfo, destination);
                        }
                        return null; // remove
                    } else {
                        logger.debug("will not expire consumer {} as it has been updated to expire at {}",
                            consumerInfo.getConsumerId(), timeSubscriptionShouldExpire);
                    }
                    return timeSubscriptionShouldExpire;
                }),
                10,
                TimeUnit.SECONDS
            );
        }
        return consumerId;
    }
}
