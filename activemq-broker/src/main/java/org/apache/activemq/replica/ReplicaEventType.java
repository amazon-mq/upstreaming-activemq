package org.apache.activemq.replica;

public enum ReplicaEventType {
    DESTINATION_UPSERT,
    DESTINATION_DELETE,
    MESSAGE_SEND,
    MESSAGE_ACK,
    MESSAGE_CONSUMED,
    MESSAGE_DISCARDED,
    MESSAGE_DROPPED,
    TRANSACTION_BEGIN,
    TRANSACTION_PREPARE,
    TRANSACTION_ROLLBACK,
    TRANSACTION_COMMIT,
    TRANSACTION_FORGET,
    MESSAGE_EXPIRED,
    SUBSCRIBER_REMOVED,
    SUBSCRIBER_ADDED;

    static final String EVENT_TYPE_PROPERTY = "ActiveMQ.Replication.EventType";
}
