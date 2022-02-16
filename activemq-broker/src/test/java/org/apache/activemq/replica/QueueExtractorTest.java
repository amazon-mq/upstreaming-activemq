package org.apache.activemq.replica;

import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class QueueExtractorTest {

    @Test
    public void extractQueueFromQueue() {
        Queue queue = mock(Queue.class);
        Queue result = QueueExtractor.extractQueue(queue);

        assertThat(result).isEqualTo(queue);
    }

    @Test
    public void extractQueueFromDestinationFilter() {
        Queue queue = mock(Queue.class);
        Queue result = QueueExtractor.extractQueue(new DestinationFilter(queue));

        assertThat(result).isEqualTo(queue);
    }

    @Test
    public void extractNullFromNonQueue() {
        Topic topic = mock(Topic.class);
        Queue result = QueueExtractor.extractQueue(topic);

        assertThat(result).isNull();
    }
}
