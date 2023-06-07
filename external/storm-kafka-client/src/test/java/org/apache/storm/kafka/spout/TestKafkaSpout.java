package org.apache.storm.kafka.spout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class TestKafkaSpout {
    private final static int VALID = 0;
    private final static int INVALID = 1;
    private final static int NULL = 2;

    private KafkaSpout<String, String> kafkaSpout;


    @BeforeEach
    public void createKafkaSpout() {

        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic").build();

        kafkaSpout = new KafkaSpout<>(conf);
        assertNotNull(kafkaSpout);
    }

    private static Map<String, Object> getConf(int type) {
        switch (type) {
            case VALID:
                return new HashMap<>();
            case INVALID:
                Map<String, Object> conf = mock(HashMap.class);
                doThrow(new RuntimeException()).when(conf).get(anyString());
                return conf;
            case NULL:
                return null;
            default:
                fail("Unexpected type.");
                return null;
        }
    }

    private static TopologyContext getTopology(int type) {
        TopologyContext topologyContext = mock(TopologyContext.class);
        switch (type) {
            case VALID:
                return topologyContext;
            case INVALID:
                doThrow(new RuntimeException("Invalid TopologyContext")).when(topologyContext).getExecutorData(any()); // TODO improve
            case NULL:
                return null;
            default:
                fail("Unexpected type.");
                return null;
        }
    }

    private static SpoutOutputCollector getCollector(int type) {
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        switch (type) {
            case VALID:
                return collector;
            case INVALID:
                doThrow(new RuntimeException("Invalid SpoutOutputCollector")).when(collector).getPendingCount(); // TODO improve
                doThrow(new RuntimeException("Invalid SpoutOutputCollector")).when(collector).emit(any()); // TODO improve
                doThrow(new RuntimeException("Invalid SpoutOutputCollector")).when(collector).emit(any(), any(), any()); // TODO improve
                doThrow(new RuntimeException("Invalid SpoutOutputCollector")).when(collector).emit(anyList(), any()); // TODO improve
                doThrow(new RuntimeException("Invalid SpoutOutputCollector")).when(collector).emit(anyString(), any()); // TODO improve
            case NULL:
                return null;
            default:
                fail("Unexpected type.");
                return null;
        }
    }

    public static Stream<Arguments> openKafkaSpoutParams() {
        return Stream.of(
                Arguments.of(getConf(VALID), getTopology(VALID), getCollector(VALID), false),
                //Arguments.of(getConf(INVALID), getTopology(VALID), getCollector(VALID), true),
                Arguments.of(getConf(VALID), getTopology(INVALID), getCollector(VALID), true),
                //Arguments.of(getConf(VALID), getTopology(VALID), getCollector(INVALID), true),
                Arguments.of(getConf(INVALID), getTopology(INVALID), getCollector(VALID), true),
                Arguments.of(getConf(VALID), getTopology(INVALID), getCollector(INVALID), true),
                //Arguments.of(getConf(INVALID), getTopology(VALID), getCollector(INVALID), true),
                Arguments.of(getConf(INVALID), getTopology(INVALID), getCollector(INVALID), true)
        );
    }


    @ParameterizedTest
    @MethodSource("openKafkaSpoutParams")
    public void testOpenKafkaSpout(Map<String, Object> conf, TopologyContext topologyContext, SpoutOutputCollector collector, boolean exceptionExpected) {
        try {
            kafkaSpout.open(conf, topologyContext, collector);
            assertFalse(exceptionExpected);
        } catch (Exception e) {
            assertTrue(exceptionExpected);
        }
    }

}
