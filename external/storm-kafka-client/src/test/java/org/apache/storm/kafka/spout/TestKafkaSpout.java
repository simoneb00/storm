package org.apache.storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.Properties;

public class TestKafkaSpout {


    @Test
    public void createKafkaSpout() {

        KafkaSpoutConfig<String, String> spoutConfig = mock(KafkaSpoutConfig.class);

        when(spoutConfig.getConsumerGroupId()).thenReturn("mock-consumer-group");
        when(spoutConfig.getRetryService()).thenReturn(mock(KafkaSpoutRetryService.class));

        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(spoutConfig);

        Assertions.assertNotNull(kafkaSpout);
    }

}
