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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.util.ByteSequence;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ReplicaEventSerializerTest {

    private final ReplicaEventSerializer serializer = new ReplicaEventSerializer();

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_DESTINATION_UPSERT() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_DESTINATION_DELETE() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_MESSAGE_SEND() throws IOException {
        var message = new ActiveMQMessage();
        fail("Need correct data for test");

        var bytes = serializer.serializeMessageData(message);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(ActiveMQMessage.class)
            .isEqualTo(message);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_MESSAGE_ACK() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_MESSAGE_CONSUMED() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_MESSAGE_DISCARDED() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_BEGIN() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_PREPARE() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_ROLLBACK() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_COMMIT() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_TRANSACTION_FORGET() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_MESSAGE_EXPIRED() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_SUBSCRIBER_REMOVED() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    @Test
    @Ignore
    public void canDoRoundTripSerializedForDataOf_SUBSCRIBER_ADDED() throws IOException {
        var object = Mockito.mock(DataStructure.class);
        var expectedClass = ActiveMQDestination.class;
        fail("Need correct object for test");

        var bytes = serializer.serializeReplicationData(object);
        var deserialized = serializer.deserializeMessageData(asSequence(bytes));

        assertThat(bytes).isNotNull();
        assertThat(deserialized).isInstanceOf(expectedClass)
            .isEqualTo(object);
    }

    private ByteSequence asSequence(byte[] bytes) {
        return new ByteSequence(bytes);
    }

}
