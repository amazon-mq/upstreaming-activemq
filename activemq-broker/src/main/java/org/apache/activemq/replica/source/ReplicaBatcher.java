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
package org.apache.activemq.replica.source;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.replica.util.ReplicaEventType;
import org.apache.activemq.replica.ReplicaPolicy;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaBatcher {

    private final ReplicaPolicy replicaPolicy;
    private static final Logger logger = LoggerFactory.getLogger(ReplicaBatcher.class);

    public ReplicaBatcher(ReplicaPolicy replicaPolicy) {
        this.replicaPolicy = replicaPolicy;
    }

    @SuppressWarnings("unchecked")
    List<List<MessageReference>> batches(List<MessageReference> list) throws Exception {
        List<List<MessageReference>> result = new ArrayList<>();

        final Map<String, Set<String>> destination2eventType = new HashMap<>();

        // Tracks SEND messages within the current batch targeting topic destinations, so we can use it to avoid putting
        // these SENDs and their corresponding ACKs in the same batch. This is important for Virtual Topics:
        //
        // Once the SEND message reaches the Replica broker it will fan out to their corresponding consumer queues. If an
        // ACK is replicated in the same batch it will result in the Replica trying to replay the ACK in the same transaction
        // as the fanned out messages. Because the transaction is still open, the messages won't be visible and the ACK will fail.
        // It results in messages never being ACKed in the replica broker.
        //
        // This is only a problem for Virtual Topics fanned out messages because their "SEND" is not replicated, instead we let the
        // Replica broker fan out them again. Therefore, the compactor will not match SENDs and ACKs.
        final Set<String> batchedMessageIdsSentToTopics = new HashSet<>();

        List<MessageReference> batch = new ArrayList<>();

        int batchSize = 0;
        for (MessageReference reference : list) {
            final ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();
            final String originalDestination = message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY);
            final ReplicaEventType currentEventType = ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));

            if (ReplicaEventType.TRANSACTIONLESS_EVENT_TYPES.contains(currentEventType)) {
                if (!batch.isEmpty()) {
                    result.add(batch);
                    batch = new ArrayList<>();
                    batchSize = 0;
                }
                batch.add(reference);
                result.add(batch);
                batch = new ArrayList<>();
                batchedMessageIdsSentToTopics.clear();
                continue;
            }

            boolean eventTypeSwitch = false;
            boolean shouldTrackTopicSend = false; // See batchedMessageIdsSentToTopics
            if (originalDestination != null) {
                final Set<String> sends = destination2eventType.computeIfAbsent(originalDestination, k -> new HashSet<>());

                if (currentEventType == ReplicaEventType.MESSAGE_SEND) {
                    sends.add(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY));

                    final boolean originalMessageWasSentToTopic = !message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY);
                    final boolean messageWasInXATransaction = message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY);
                    if (originalMessageWasSentToTopic && !messageWasInXATransaction) {
                        logger.trace("Message {} was sent to a Topic Destination: {}", message, originalDestination);
                        shouldTrackTopicSend = true;
                    }
                }
                if (currentEventType == ReplicaEventType.MESSAGE_ACK) {
                    final List<String> messageIds = (List<String>) message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY);
                    if (sends.stream().anyMatch(messageIds::contains)) {
                        destination2eventType.put(originalDestination, new HashSet<>());
                        eventTypeSwitch = true;
                    }  else {
                        eventTypeSwitch = batchedMessageIdsSentToTopics.stream() // Prevents topic SEND-ACK pairs from being batched together.
                                .filter(messageIds::contains)
                                .peek(messageId -> logger.trace("Message {} was ACK to a previous topic SEND: {}", messageId, originalDestination))
                                .findAny().isPresent();
                    }
                }
            }

            boolean exceedsLength = batch.size() + 1 > replicaPolicy.getMaxBatchLength();
            boolean exceedsSize = batchSize + reference.getSize() > replicaPolicy.getMaxBatchSize();
            if (!batch.isEmpty() && (exceedsLength || exceedsSize || eventTypeSwitch)) {
                result.add(batch);
                batch = new ArrayList<>();
                batchSize = 0;
                batchedMessageIdsSentToTopics.clear();
                logger.trace("Building a new batch");
            }

            if (shouldTrackTopicSend) {
                // Must be done after checking if the batch as exceeded the size limit. Otherwise, this message ID may not be tracked.
                batchedMessageIdsSentToTopics.add(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY));
            }

            batch.add(reference);
            batchSize += reference.getSize();
        }
        if (!batch.isEmpty()) {
            result.add(batch);
        }

        return result;
    }
}
