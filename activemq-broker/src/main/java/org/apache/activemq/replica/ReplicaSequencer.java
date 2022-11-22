/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.jmx.ReplicationView;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ReplicaSequencer implements Task {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSequencer.class);

    private static final String SOURCE_CONSUMER_CLIENT_ID = "DUMMY_SOURCE_CONSUMER";
    private static final String ACK_SELECTOR = String.format("%s LIKE '%s'", ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK);
    static final int MAX_BATCH_LENGTH = 500;
    static final int MAX_BATCH_SIZE = 5_000_000; // 5 Mb
    public static final int ITERATE_PERIOD = 5_000;
    public static final int MAXIMUM_MESSAGES = 1_000;

    private final Broker broker;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicationMessageProducer replicationMessageProducer;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final Object iteratingMutex = new Object();
    private final AtomicLong pendingWakeups = new AtomicLong();
    private final AtomicLong pendingTriggeredWakeups = new AtomicLong();
    final Set<String> deliveredMessages = new HashSet<>();
    final LinkedList<String> messageToAck = new LinkedList<>();
    private final ReplicaStorage replicaStorage;
    private final ReplicaAckHelper replicaAckHelper;

    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator customerIdGenerator = new LongSequenceGenerator();
    private TaskRunner taskRunner;
    private Queue intermediateQueue;
    private Queue mainQueue;
    private ConnectionContext connectionContext;

    private PrefetchSubscription subscription;
    private boolean hasConsumer;

    BigInteger sequence = BigInteger.ZERO;
    MessageId recoveryMessageId;

    private final AtomicLong lastProcessTime = new AtomicLong();

    private final AtomicBoolean initialized = new AtomicBoolean();

    private ReplicationView replicationView;
    private final AtomicLong counter = new AtomicLong();
    private long lastCounter;

    public ReplicaSequencer(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicationMessageProducer replicationMessageProducer) {
        this.broker = broker;
        this.queueProvider = queueProvider;
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaStorage = new ReplicaStorage("source_sequence");
        this.replicaAckHelper = new ReplicaAckHelper(broker);

        if (broker.getBrokerService().isUseJmx()) {
            replicationView = new ReplicationView();
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                long c = counter.get();
                replicationView.setReplicationTps((c - lastCounter) / 10);
                lastCounter = c;
            }, 10, 10, TimeUnit.SECONDS);
        }

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::asyncWakeup,
                ITERATE_PERIOD, ITERATE_PERIOD, TimeUnit.MILLISECONDS);
    }

    void initialize() throws Exception {
        BrokerService brokerService = broker.getBrokerService();
        TaskRunnerFactory taskRunnerFactory = brokerService.getTaskRunnerFactory();
        taskRunner = taskRunnerFactory.createTaskRunner(this, "ReplicationPlugin.Sequencer");

        intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        mainQueue = broker.getDestinations(queueProvider.getMainQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setClientId(SOURCE_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection() {
            @Override
            public void dispatchAsync(Command command) {
                asyncWakeup();
            }

            @Override
            public void dispatchSync(Command message) {
                asyncWakeup();
            }
        });
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.Sequencer").generateId());
        SessionId sessionId = new SessionId(connectionId, sessionIdGenerator.getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, customerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10000);
        consumerInfo.setDestination(queueProvider.getIntermediateQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);

        replicaStorage.initialize(new File(brokerService.getBrokerDataDirectory(),
                ReplicaSupport.REPLICATION_PLUGIN_STORAGE_DIRECTORY));

        restoreSequence();

        initialized.compareAndSet(false, true);
        asyncWakeup();

        if (brokerService.isUseJmx()) {
            AnnotatedMBean.registerMBean(brokerService.getManagementContext(), replicationView, createJmxName());
        }
    }

    private ObjectName createJmxName() throws MalformedObjectNameException {
        String objectNameStr = broker.getBrokerService().getBrokerObjectName().toString();

        objectNameStr += "," + "service=Plugins";
        objectNameStr += "," + "instanceName=ReplicationPlugin";

        return new ObjectName(objectNameStr);
    }

    void restoreSequence() throws Exception {
        String line = replicaStorage.read();
        if (line == null) {
            return;
        }
        String[] split = line.split("#");
        if (split.length != 2) {
            return;
        }
        sequence = new BigInteger(split[0]);

        recoveryMessageId = new MessageId(split[1]);
        int index = intermediateQueue.getAllMessageIds().indexOf(recoveryMessageId);
        if (index == -1) {
            return;
        }

        sequence = sequence.subtract(BigInteger.valueOf(index + 1));
    }

    @SuppressWarnings("unchecked")
    void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        List<MessageReference> messagesToAck = replicaAckHelper.getMessagesToAck(ack, mainQueue);

        if (messagesToAck == null || messagesToAck.isEmpty()) {
            throw new IllegalStateException("Could not find messages for ack");
        }
        List<String> messageIds = new ArrayList<>();
        for (MessageReference reference : messagesToAck) {
            messageIds.addAll((List<String>) reference.getMessage().getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));
        }

        broker.acknowledge(consumerExchange, ack);
        synchronized (messageToAck) {
            messageIds.forEach(messageToAck::addLast);
        }
        asyncWakeup();
    }

    void asyncWakeup() {
        try {
            long l = pendingWakeups.incrementAndGet();
            if (l % 500 == 0) {
                pendingTriggeredWakeups.incrementAndGet();
                taskRunner.wakeup();
                pendingWakeups.addAndGet(-500);
                return;
            }

            if (System.currentTimeMillis() - lastProcessTime.get() > ITERATE_PERIOD) {
                pendingTriggeredWakeups.incrementAndGet();
                taskRunner.wakeup();
            }
        } catch (InterruptedException e) {
            logger.warn("Async task runner failed to wakeup ", e);
        }
    }

    @Override
    public boolean iterate() {
        synchronized (iteratingMutex) {
            lastProcessTime.set(System.currentTimeMillis());
            if (!initialized.get()) {
                return false;
            }

            iterateAck();
            iterateSend();

            if (pendingTriggeredWakeups.get() > 0) {
                pendingTriggeredWakeups.decrementAndGet();
            }
        }

        return pendingTriggeredWakeups.get() > 0;
    }

    void iterateAck() {
        MessageAck ack = new MessageAck();
        List<String> messages;
        synchronized (messageToAck) {
            if (!messageToAck.isEmpty()) {
                ack.setFirstMessageId(new MessageId(messageToAck.getFirst()));
                ack.setLastMessageId(new MessageId(messageToAck.getLast()));
                ack.setMessageCount(messageToAck.size());
                ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
                ack.setDestination(queueProvider.getIntermediateQueue());
            }
            messages = new ArrayList<>(messageToAck);
        }

        if (!messages.isEmpty()) {
            try {
                TransactionId transactionId = new LocalTransactionId(
                        new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        localTransactionIdGenerator.getNextSequenceId());
                ack.setTransactionId(transactionId);

                synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
                    broker.beginTransaction(connectionContext, transactionId);

                    ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
                    consumerExchange.setConnectionContext(connectionContext);
                    consumerExchange.setSubscription(subscription);

                    broker.acknowledge(consumerExchange, ack);

                    broker.commitTransaction(connectionContext, transactionId, true);
                }
                counter.addAndGet(messages.size());

                synchronized (messageToAck) {
                    messageToAck.removeAll(messages);
                }

                synchronized (deliveredMessages) {
                    messages.forEach(deliveredMessages::remove);
                }

                asyncWakeup();
            } catch (Exception e) {
                logger.error("Could not acknowledge replication messages", e);
            }
        }
    }

    void iterateSend() {
        List<MessageReference> dispatched = subscription.getDispatched();
        List<MessageReference> toProcess = new ArrayList<>();
        List<String> dispatchedMessageIds = new ArrayList<>();

        MessageReference recoveryMessage = null;

        synchronized (deliveredMessages) {
            Collections.reverse(dispatched);
            for (MessageReference reference : dispatched) {
                MessageId messageId = reference.getMessageId();
                dispatchedMessageIds.add(messageId.toString());
                if (deliveredMessages.contains(messageId.toString())) {
                    break;
                }

                ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();

                if (message.getTargetConsumerId() == null) {
                    message.setTargetConsumerId(subscription.getConsumerInfo().getConsumerId());
                }

                toProcess.add(reference);
                if (messageId.equals(recoveryMessageId)) {
                    recoveryMessage = reference;
                }
            }
        }

        if (!hasConsumer) {
            try {
                List<MessageReference> messagesToCompact = intermediateQueue.getMatchingMessages(connectionContext, ACK_SELECTOR, MAXIMUM_MESSAGES);
                if (!messagesToCompact.isEmpty()) {
                    String selector = String.format("%s IN %s", ReplicaSupport.MESSAGE_ID_PROPERTY,  getAckedMessageIds(messagesToCompact));
                    messagesToCompact.addAll(intermediateQueue.getMatchingMessages(connectionContext, selector, MAXIMUM_MESSAGES));
                }

                for (MessageReference messageReference : messagesToCompact) {
                    String messageId = messageReference.getMessageId().toString();
                    if (!dispatchedMessageIds.contains(messageId)) {
                        messageDispatch(messageReference.getMessageId());
                        toProcess.add(messageReference);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to get matching messages", e);
            }
        }

        if (toProcess.isEmpty()) {
            return;
        }

        Collections.reverse(toProcess);

        if (recoveryMessage != null) {
            toProcess = toProcess.subList(0, toProcess.indexOf(recoveryMessage) + 1);
        }

        if (recoveryMessageId == null) {
            try {
                toProcess = compactAndFilter(toProcess);
            } catch (Exception e) {
                logger.error("Filed to compact messages in the intermediate replication queue", e);
                return;
            }
        }

        if (!hasConsumer) {
            return;
        }

        List<List<MessageReference>> batches;
        try {
            batches = batches(toProcess);
        } catch (Exception e) {
            logger.error("Filed to batch messages in the intermediate replication queue", e);
            return;
        }

        MessageId lastProcessedMessageId = null;
        for (List<MessageReference> batch : batches) {
            try {
                List<String> messageIds = new ArrayList<>();
                List<DataStructure> messages = new ArrayList<>();
                for (MessageReference reference : batch) {
                    ActiveMQMessage originalMessage = (ActiveMQMessage) reference.getMessage();
                    sequence = sequence.add(BigInteger.ONE);

                    ActiveMQMessage message = (ActiveMQMessage) originalMessage.copy();

                    message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, sequence.toString());

                    message.setDestination(null);
                    message.setTransactionId(null);
                    message.setPersistent(false);

                    messageIds.add(reference.getMessageId().toString());
                    messages.add(message);
                }

                ReplicaEvent replicaEvent = new ReplicaEvent()
                        .setEventType(ReplicaEventType.BATCH)
                        .setEventData(eventSerializer.serializeListOfObjects(messages))
                        .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIds);

                replicationMessageProducer.enqueueMainReplicaEvent(connectionContext, replicaEvent);

                synchronized (deliveredMessages) {
                    deliveredMessages.addAll(messageIds);
                }
                lastProcessedMessageId = batch.get(batch.size() - 1).getMessageId();
            } catch (Exception e) {
                sequence = sequence.subtract(BigInteger.valueOf(batch.size()));
                logger.error("Filed to persist message in the main replication queue", e);
                break;
            }
        }

        if (lastProcessedMessageId != null) {
            try {
                replicaStorage.write(sequence.toString() + "#" + lastProcessedMessageId);
            } catch (Exception e) {
                logger.error("Filed to write source sequence to disk", e);
            }
        }

        if (recoveryMessage != null) {
            recoveryMessageId = null;
        }
    }

    void updateMainQueueConsumerStatus() {
        try {
            if (!hasConsumer && !mainQueue.getConsumers().isEmpty()) {
                hasConsumer = true;
                asyncWakeup();
            } else if (hasConsumer && mainQueue.getConsumers().isEmpty()) {
                hasConsumer = false;
            }
        } catch (Exception error) {
            logger.error("Failed to update replica consumer count.", error);
        }
    }

    List<List<MessageReference>> batches(List<MessageReference> list) throws JMSException {
        List<List<MessageReference>> result = new ArrayList<>();

        Map<String, ReplicaEventType> destination2eventType = new HashMap<>();
        List<MessageReference> batch = new ArrayList<>();
        int batchSize = 0;
        for (MessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();
            String originalDestination = message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY);

            boolean eventTypeSwitch = false;
            if (originalDestination != null) {
                ReplicaEventType currentEventType =
                        ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
                ReplicaEventType lastEventType = destination2eventType.put(originalDestination, currentEventType);
                if (lastEventType == ReplicaEventType.MESSAGE_SEND && currentEventType == ReplicaEventType.MESSAGE_ACK) {
                    eventTypeSwitch = true;
                }
            }

            boolean exceedsLength = batch.size() + 1 > MAX_BATCH_LENGTH;
            boolean exceedsSize = batchSize + reference.getSize() > MAX_BATCH_SIZE;
            if (batch.size() > 0 && (exceedsLength || exceedsSize || eventTypeSwitch)) {
                result.add(batch);
                batch = new ArrayList<>();
                batchSize = 0;
            }

            batch.add(reference);
            batchSize += reference.getSize();
        }
        if (batch.size() > 0) {
            result.add(batch);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    List<MessageReference> compactAndFilter(List<MessageReference> list) throws Exception {
        List<MessageReference> result = new ArrayList<>(list);
        Map<String, MessageId> sendMap = new LinkedHashMap<>();
        Map<List<String>, ActiveMQMessage> ackMap = new LinkedHashMap<>();
        for (MessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();

            if (!message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY)
                    || message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY)) {
                continue;
            }

            ReplicaEventType eventType =
                    ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
            if (eventType == ReplicaEventType.MESSAGE_SEND) {
                sendMap.put(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY), message.getMessageId());
            }
            if (eventType == ReplicaEventType.MESSAGE_ACK) {
                List<String> messageIds = (List<String>)
                        Optional.ofNullable(message.getProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY))
                                .orElse(message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));

                ackMap.put(messageIds, message);
            }
        }

        List<MessageId> toDelete = new ArrayList<>();

        for (Map.Entry<List<String>, ActiveMQMessage> ack : ackMap.entrySet()) {
            List<String> sends = new ArrayList<>();
            List<String> messagesToAck = ack.getKey();
            for (String id : messagesToAck) {
                if (sendMap.containsKey(id)) {
                    sends.add(id);
                    toDelete.add(sendMap.get(id));
                }
            }
            if (sends.size() == 0) {
                continue;
            }

            ActiveMQMessage message = ack.getValue();
            if (messagesToAck.size() == sends.size() && new HashSet<>(messagesToAck).containsAll(sends)) {
                toDelete.add(message.getMessageId());
                continue;
            }

            message.setProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY, messagesToAck);
            ArrayList<String> newList = new ArrayList<>(messagesToAck);
            newList.removeAll(sends);
            message.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, newList);
            message.setTargetConsumerId(null);

            synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
                intermediateQueue.getMessageStore().updateMessage(message);
            }
        }

        if (toDelete.isEmpty()) {
            return result;
        }

        TransactionId transactionId = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                localTransactionIdGenerator.getNextSequenceId());

        synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
            broker.beginTransaction(connectionContext, transactionId);

            ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
            consumerExchange.setConnectionContext(connectionContext);

            for (MessageId id : toDelete) {
                MessageAck ack = new MessageAck();
                ack.setMessageID(id);
                ack.setMessageCount(1);
                ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
                ack.setDestination(queueProvider.getIntermediateQueue());
                consumerExchange.setSubscription(subscription);
                broker.acknowledge(consumerExchange, ack);
            }

            broker.commitTransaction(connectionContext, transactionId, true);
        }

        result.removeIf(reference -> toDelete.contains(reference.getMessageId()));

        counter.addAndGet(toDelete.size());
        return result;
    }

    private String getAckedMessageIds(List<MessageReference> ackMessages) {
        return ackMessages.stream()
                .map(messageReference -> {
                    List<String> messageIds = new ArrayList<>();
                    try {
                        ActiveMQMessage message = (ActiveMQMessage) messageReference.getMessage();

                        messageIds.addAll((List<String>) Optional.ofNullable(message.getProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY))
                                .orElse(message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY)));
                    } catch (Exception error) {
                        logger.error("Failed to get message ids of acked messages", error);
                    }
                    return messageIds;
                }).flatMap(Collection::stream).collect(Collectors.joining("','", "('", "')"));
    }

    private void messageDispatch(MessageId messageId) throws Exception {
        MessageDispatchNotification mdn = new MessageDispatchNotification();
        mdn.setConsumerId(subscription.getConsumerInfo().getConsumerId());
        mdn.setDestination(queueProvider.getIntermediateQueue());
        mdn.setMessageId(messageId);
        broker.processDispatchNotification(mdn);
    }
}
