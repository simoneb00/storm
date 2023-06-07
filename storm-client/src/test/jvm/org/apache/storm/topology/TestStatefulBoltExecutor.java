package org.apache.storm.topology;


import org.apache.storm.generated.Bolt;
import org.apache.storm.spout.CheckPointState;
import org.apache.storm.state.KeyValueState;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

public class TestStatefulBoltExecutor {

    private static final int VALID = 0;
    private static final int INVALID = 1;
    private static final int NULL = 2;

    @Mock
    private IStatefulBolt<KeyValueState<String, String>> mockedBolt;
    private StatefulBoltExecutor executor;
    private Map<String, Object> topoConf = new HashMap<>();
    @Mock
    private TopologyContext context;
    @Mock
    private OutputCollector outputCollector;


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mockedBolt = mock(IStatefulBolt.class);
        executor = new StatefulBoltExecutor<>(mockedBolt);
        assertNotNull(executor);
        executor.prepare(topoConf, context, outputCollector);
    }

    private static Map<String, Object> getTopoConf(int type) {
        switch (type) {
            case VALID:
                return new HashMap<>();
            case INVALID:
                Map<String, Object> map = mock(HashMap.class);
                doThrow(new RuntimeException("Invalid configuration")).when(map).get(any());
                return map;
            case NULL:
                return null;
            default:
                fail("Unexpected type.");
                return null;
        }
    }

    private static TopologyContext getContext(int type) {
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
                fail("Unexpected type.");
                return null;
        }
    }

    private static OutputCollector getOutputCollector(int type) {
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
                fail("Unexpected type.");
                return null;
        }
    }

    private static Stream<Arguments> prepareParams() {
        return Stream.of(
                Arguments.of(getTopoConf(VALID), getContext(VALID), getOutputCollector(VALID), false),
                //Arguments.of(getTopoConf(INVALID), getContext(VALID), getOutputCollector(VALID), true), TODO
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getOutputCollector(VALID), true),
                //Arguments.of(getTopoConf(INVALID), getContext(VALID), getOutputCollector(INVALID), true), TODO
                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getOutputCollector(INVALID), true)
        );
    }

    @ParameterizedTest
    @MethodSource("prepareParams")
    public void testPrepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector outputCollector, boolean exceptionExpected) {
        try {
            executor.prepare(topoConf, context, outputCollector);
            assertFalse(exceptionExpected);
            Map<String, Object> actualTopoConf = executor.getComponentConfiguration();
            assertEquals(topoConf, actualTopoConf);
        } catch (RuntimeException e) {
            assertTrue(exceptionExpected);
        }
    }

    @Test
    public void testHandleTupleWithoutBoltInitialization() {

        Tuple mockedTuple = mock(Tuple.class);

        /* no bolt initialization, so we don't expect the bolt to execute */
        executor.handleTuple(mockedTuple);
        verify(mockedBolt, times(0)).execute(mockedTuple);
    }

    @Test
    public void testHandleTupleWithBoltInitialization() {

        Tuple mockedTuple = mock(Tuple.class);

        /* here we use the checkpoint mechanism to set the bolt to the state "initialized" */
        executor.handleCheckpoint(mockedTuple, CheckPointState.Action.INITSTATE, 0);

        executor.handleTuple(mockedTuple);
        verify(mockedBolt, times(1)).execute(mockedTuple);
    }

    /* we want to test the handleCheckpoint method, wrt the action INITSTATE */
    @Test
    public void testInitBoltCheckpoint() {

        Tuple tuple1 = mock(Tuple.class);
        Tuple tuple2 = mock(Tuple.class);
        Tuple tuple3 = mock(Tuple.class);

        /* the bolt is not initialized, so if we try to execute it on some tuples, the executions won't occur and will become pending */
        executor.handleTuple(tuple1);
        executor.handleTuple(tuple2);
        executor.handleTuple(tuple3);

        verify(mockedBolt, times(0)).execute(tuple1);
        verify(mockedBolt, times(0)).execute(tuple2);
        verify(mockedBolt, times(0)).execute(tuple3);

        /* let's add a checkpoint to the action INITSTATE */
        executor.handleCheckpoint(tuple1, CheckPointState.Action.INITSTATE, 123);

        /* the method initState must be called to initiate the bolt */
        verify(mockedBolt, times(1)).initState(any(KeyValueState.class));

        /* The 3 pending operations are finally executed */
        verify(mockedBolt, times(3)).execute(any(Tuple.class));
    }
}
