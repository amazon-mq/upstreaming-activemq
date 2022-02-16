package org.apache.activemq.replica;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;

public class QueueExtractor {

    static Queue extractQueue(Destination destination) {
        Destination result = destination;
        while (result != null && !(result instanceof Queue)) {
            if (result instanceof DestinationFilter) {
                result = ((DestinationFilter) result).getNext();
            } else {
                return null;
            }
        }
        return (Queue) result;
    }
}
