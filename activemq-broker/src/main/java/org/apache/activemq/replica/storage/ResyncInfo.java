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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;

import java.io.Serializable;

public class ResyncInfo implements Serializable {

    private final ActiveMQDestination destination;

    private final String messageId;

    public ResyncInfo(ActiveMQDestination destination, MessageId messageId) {
        this.destination = destination;
        this.messageId = messageId.toString();
    }


    public ActiveMQDestination getDestination() {
        return destination;
    }

    public MessageId getMessageId() {
        return new MessageId(messageId);
    }
}
