package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ReplicaSourceBaseBroker extends BrokerFilter {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBaseBroker.class);
    final ReplicaReplicationQueueSupplier queueProvider;
    private ReplicationMessageProducer replicationMessageProducer;
    protected final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final AtomicBoolean initialized = new AtomicBoolean();

    private final AtomicLong executionCounts = new AtomicLong();
    private final AtomicLong totalExecutionTime = new AtomicLong();

    ReplicaSourceBaseBroker(Broker next) {
        super(next);
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        queueProvider.initialize();
        initialized.compareAndSet(false, true);

        ReplicaInternalMessageProducer replicaInternalMessageProducer = new ReplicaInternalMessageProducer(next, getAdminConnectionContext());
        replicationMessageProducer = new ReplicationMessageProducer(replicaInternalMessageProducer, queueProvider);
        super.start();
    }


    protected void enqueueReplicaEvent(ConnectionContext initialContext, ReplicaEvent event) throws Exception {
        final long startNano = System.nanoTime();
        if (isReplicaContext(initialContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }
        replicationMessageProducer.enqueueReplicaEvent(event);
        long currentExecution = executionCounts.addAndGet(1);
        long currentTotalTime = totalExecutionTime.addAndGet(System.nanoTime() - startNano);
        if (currentExecution % 1000 == 0) {
            logger.error("[enqueueReplicaEvent] average execution time for [{}] messages is [{}]ns", currentExecution, currentTotalTime / currentExecution);
        }
    }

    protected boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

}
