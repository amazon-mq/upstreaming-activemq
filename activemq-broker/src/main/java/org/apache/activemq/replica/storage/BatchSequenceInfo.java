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
package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BatchSequenceInfo implements Serializable {

    private final String sequence;

    private final String[] messageIds;

    public BatchSequenceInfo(BigInteger sequence, List<MessageReference> messageIds) {
        this.sequence = sequence.toString();
        this.messageIds = messageIds.stream().map(MessageReference::getMessageId).map(MessageId::toString).toArray(String[]::new);
    }

    public BigInteger getSequence() {
        return new BigInteger(sequence);
    }

    public List<MessageId> getMessageIds() {
        return Arrays.stream(messageIds).map(MessageId::new).collect(Collectors.toList());
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "BatchSequenceInfo{" +
                "sequence='" + sequence + '\'' +
                ", messageIds=" + java.util.Arrays.toString(messageIds) +
                '}';
    }
}
