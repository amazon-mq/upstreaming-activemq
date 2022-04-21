package org.apache.activemq.replica;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;

public class DestinationExtractor {

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

    static Topic extractTopic(Destination destination) {
        Destination result = destination;
        while (result != null && !(result instanceof Topic)) {
            if (result instanceof DestinationFilter) {
                result = ((DestinationFilter) result).getNext();
            } else {
                return null;
            }
        }
        return (Topic) result;
    }
}
