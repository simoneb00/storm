package org.apache.storm.utils;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestingUtils {

    public static final int VALID = 0;
    public static final int INVALID = 1;
    public static final int NULL = 2;

    public static Map<String, Object> getTopoConf(int type) {
        switch (type) {
            case VALID:
                return new HashMap<>();
            case INVALID:
                Map<String, Object> map = mock(HashMap.class);
                doThrow(new RuntimeException("Invalid configuration")).when(map).get(any());
                doThrow(new RuntimeException("Invalid configuration")).when(map).containsKey(any());
                doThrow(new RuntimeException("Invalid configuration")).when(map).put(any(), any());
                doThrow(new RuntimeException("Invalid configuration")).when(map).keySet();
                doThrow(new RuntimeException("Invalid configuration")).when(map).entrySet();
                return map;
            case NULL:
                return null;
            default:
                fail("Unexpected type");
                return null;
        }
    }

    public static TopologyContext getContext(int type) {
        TopologyContext topologyContext = mock(TopologyContext.class);
        switch (type) {
            case VALID:
                return topologyContext;
            case INVALID:
                doThrow(new RuntimeException("Invalid context")).when(topologyContext).getExecutorData(anyString());
                doThrow(new RuntimeException("Invalid context")).when(topologyContext).getHooks();
                doThrow(new RuntimeException("Invalid context")).when(topologyContext).getNodeToHost();
                doThrow(new RuntimeException("Invalid context")).when(topologyContext).getThisComponentId();
                return topologyContext;
            case NULL:
                return null;
            default:
                fail("Unexpected type");
                return null;
        }
    }

    public static OutputCollector getOutputCollector(int type) {
        OutputCollector outputCollector = mock(OutputCollector.class);
        switch (type) {
            case VALID:
                return outputCollector;
            case INVALID:
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyString(), any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyCollection(), any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(any(), anyCollection(), any());
                return outputCollector;
            case NULL:
                return null;
            default:
                fail("Unexpected type");
                return null;
        }
    }

    public static SpoutOutputCollector getSpoutOutputCollector(int type) {
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class);
        switch (type) {
            case VALID:
                return outputCollector;
            case INVALID:
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyList());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyList(), any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyString(), anyList(), any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyString(), anyList());
                return outputCollector;
            case NULL:
                return null;
            default:
                fail("Unexpected type");
                return null;
        }
    }
}
