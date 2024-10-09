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
package org.apache.activemq.replica.source.resync;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.storage.ReplicaResynchronizationStorage;
import org.apache.activemq.replica.storage.ResyncInfo;
import org.apache.activemq.replica.util.ReplicaSupport;
import org.apache.activemq.store.MessageRecoveryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReplicaMessageRecoveryListener implements MessageRecoveryListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaMessageRecoveryListener.class);

    private final Broker broker;
    final ConnectionContext connectionContext;
    private final ReplicaResynchronizationStorage storage;
    private final ActiveMQDestination destination;
    private final MessageId restoreMessageId;
    private final RecoveryListener listener;
    private int processedCount;

    private boolean foundMessage;

    private TransactionId tid;

    private MessageId lastProcessedMessageId;

    ReplicaMessageRecoveryListener(Broker broker, ReplicaResynchronizationStorage storage, ConnectionContext connectionContext,
            ActiveMQDestination destination, MessageId restoreMessageId, RecoveryListener listener) {
        this.broker = broker;
        this.connectionContext = connectionContext;
        this.storage = storage;
        this.destination = destination;
        this.restoreMessageId = restoreMessageId;
        this.listener = listener;
    }

    @Override
    public boolean recoverMessage(Message message) throws Exception {
        if (++processedCount % 10000 == 0) {
            logger.info("Resynchronization progress: " + processedCount);
        }

        if (restoreMessageId != null && !foundMessage) {
            if (restoreMessageId.equals(message.getMessageId())) {
                foundMessage = true;
            }
            return true;
        }

        if (tid == null) {
            tid = new LocalTransactionId(
                    new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

            broker.beginTransaction(connectionContext, tid);
        }

        try {
            listener.processMessage(message, tid);
            lastProcessedMessageId = message.getMessageId();
            if (processedCount % 100 == 0) {
                commit();
            }
        } catch (Exception e) {
            broker.rollbackTransaction(connectionContext, tid);
            logger.error("Failed to process next message", e);
            throw e;
        }
        return true;
    }


    @Override
    public boolean recoverMessageReference(MessageId ref) throws Exception {
        return false;
    }

    @Override
    public boolean hasSpace() {
        return true;
    }

    @Override
    public boolean isDuplicate(MessageId ref) {
        return false;
    }

    void commitIfNeeded() throws Exception {
        if (tid == null) {
            return;
        }
        commit();
    }

    private void commit() throws Exception {
        storage.enqueue(connectionContext, tid, new ResyncInfo(destination, lastProcessedMessageId));
        broker.commitTransaction(connectionContext, tid, true);
        tid = null;
    }

    public interface RecoveryListener {
        void processMessage(Message message, TransactionId tid) throws Exception;
    }
}
