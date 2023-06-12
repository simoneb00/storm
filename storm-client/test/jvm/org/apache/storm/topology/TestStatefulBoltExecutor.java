package org.apache.storm.topology;


import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.spout.CheckPointState;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.storm.spout.CheckpointSpout.*;
import static org.apache.storm.utils.TestingUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * This class tests the functionalities exported by the class {@link StatefulBoltExecutor} and the abstract class {@link BaseStatefulBoltExecutor}.
 */
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

    private GlobalStreamId globalStreamId = new GlobalStreamId("checkpoint_stream", CHECKPOINT_STREAM_ID);  // id of the checkpoint stream



    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mockedBolt = mock(IStatefulBolt.class);
        executor = new StatefulBoltExecutor<>(mockedBolt);
        assertNotNull(executor);

        /* the following initialization is needed in order to have only one input checkpoint stream */
        Map<GlobalStreamId, Grouping> map = new HashMap<>();
        map.put(globalStreamId, mock(Grouping.class));
        when(context.getThisSources()).thenReturn(map);
        ArrayList<Integer> dummyList = new ArrayList<>();
        dummyList.add(1);
        when(context.getComponentTasks(anyString())).thenReturn(dummyList);

        executor.prepare(topoConf, context, outputCollector);
    }

    /*  The commented out configurations fail: OutputCollector is never checked to be non-null or valid */
    private static Stream<Arguments> prepareParams() {
        return Stream.of(
                Arguments.of(getTopoConf(VALID), getContext(VALID), getOutputCollector(VALID), false),
                //Arguments.of(getTopoConf(VALID), getContext(VALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(VALID), getContext(NULL), getOutputCollector(VALID), true),
                //Arguments.of(getTopoConf(VALID), getContext(VALID), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(VALID), getContext(NULL), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(VALID), getContext(NULL), getOutputCollector(INVALID), true),

                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(VALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(VALID), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(INVALID), getContext(NULL), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(NULL), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(INVALID), getContext(NULL), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(INVALID), getContext(VALID), getOutputCollector(NULL), true),

                Arguments.of(getTopoConf(NULL), getContext(NULL), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(NULL), getContext(VALID), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(NULL), getContext(NULL), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(NULL), getContext(VALID), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(NULL), getContext(INVALID), getOutputCollector(NULL), true),
                Arguments.of(getTopoConf(NULL), getContext(NULL), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(NULL), getContext(INVALID), getOutputCollector(INVALID), true),
                Arguments.of(getTopoConf(NULL), getContext(INVALID), getOutputCollector(VALID), true),
                Arguments.of(getTopoConf(NULL), getContext(VALID), getOutputCollector(INVALID), true)
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
        executor.execute(mockedTuple);
        verify(mockedBolt, times(0)).execute(mockedTuple);
    }


    /*  Actions:
     *  - INITSTATE: initialize the state
     *  - PREPARE: prepare a transaction for the commit
     *  - COMMIT: commit a previously prepared transaction
     *  - ROLLBACK: rollback the previously prepared transaction(s)
     */


    /* test of methods processCheckpoint and handleCheckpoint, wrt the action INITSTATE, through the invocation of execute() on a checkpoint tuple */
    @Test
    public void testInitBoltCheckpoint() {

        /* Expected behavior:
         * - if the bolt's state is not initialized, every attempt of execution will fail
         * - once a bolt is initialized, it's possible to execute it over tuples, and every pending execution (i.e., executions requested when the bolt was not initialized) will be fulfilled */

        Tuple tuple1 = mock(Tuple.class);
        Tuple tuple2 = mock(Tuple.class);
        Tuple tuple3 = mock(Tuple.class);

        /* the bolt is not initialized, so if we try to execute it on some tuples, the executions won't occur and will become pending */
        executor.execute(tuple1);
        executor.execute(tuple2);
        executor.execute(tuple3);

        verify(mockedBolt, times(0)).execute(tuple1);
        verify(mockedBolt, times(0)).execute(tuple2);
        verify(mockedBolt, times(0)).execute(tuple3);

        /* let's simulate the case in which the input tuple is a checkpoint tuple, for the action INITSTATE */
        Tuple mockedCheckpointTuple = mock(Tuple.class);
        when(mockedCheckpointTuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID); // checkpoint tuple: the checkpoint tuples flow through a dedicated internal stream called $checkpoint
        when(mockedCheckpointTuple.getValueByField(CheckpointSpout.CHECKPOINT_FIELD_ACTION)).thenReturn(CheckPointState.Action.INITSTATE);

        AtomicBoolean hasInit = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            hasInit.set(true);
            return null;
        }).when(outputCollector).ack(mockedCheckpointTuple);

        /* this call must inject a checkpoint to the action INITSTATE, so the bolt's state should be initialized and all the pending operations should be executed */
        executor.execute(mockedCheckpointTuple);

        assertTrue(hasInit.get());

        /* the method initState must be called to initiate the bolt */
        verify(mockedBolt, times(1)).initState(any(KeyValueState.class));

        /* The 3 pending operations are finally executed */
        verify(mockedBolt, times(1)).execute(tuple1);
        verify(mockedBolt, times(1)).execute(tuple2);
        verify(mockedBolt, times(1)).execute(tuple3);
    }

    /* test of methods processCheckpoint and handleCheckpoint, wrt to the actions PREPARE and COMMIT, through the invocation of execute() on checkpoint tuples */
    @Test
    public void testPrepareAndCommitCheckpoint() {

        /*  The action PREPARE prepares a transaction to be committed, while the action COMMIT actually commits the transaction.
         *  It is important that, when these actions are requested, the methods bolt.prePrepare() and bolt.preCommit(), as well as the methods state.prepareCommit() and state.commit() are invoked,
         *  in order to allow the executions of preparatory operations respectively on the bolt and on the state (the latter is crucial for obvious fault tolerance reasons)
         *
         */

        Tuple mockedTuple = mock(Tuple.class);
        State state = mock(State.class);

        executor.prepare(topoConf, context, outputCollector, state);    // we re-prepare the executor, specifying the state, to test the interaction with the state

        /* expected behavior:
         * - if we execute the action PREPARE, without initializing the bolt -> fail
         * - if we execute the action INITSTATE, then PREPARE and COMMIT -> the methods bolt.prePrepare(), state.prepareCommit(), bolt.preCommit and state.commit() are correctly invoked, with the correct txid,
         *   and the action is successfully acked to the OutputCollector.
         */

        /* try without initialization, expected result: fail propagated to the output collector */
        when(mockedTuple.getSourceStreamId()).thenReturn(CHECKPOINT_STREAM_ID);
        when(mockedTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(CheckPointState.Action.PREPARE);
        executor.execute(mockedTuple);
        verify(mockedBolt, times(0)).prePrepare(anyLong());
        verify(state, times(0)).prepareCommit(anyLong());
        verify(outputCollector).fail(mockedTuple);

        /* state initialization */
        when(mockedTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(CheckPointState.Action.INITSTATE);
        executor.execute(mockedTuple);


        /* prepare: the method bolt.prePrepare() should be invoked, with the txid specified */
        when(mockedTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(CheckPointState.Action.PREPARE);
        when(mockedTuple.getLongByField(CHECKPOINT_FIELD_TXID)).thenReturn(12345L);
        AtomicBoolean hasPrepared = new AtomicBoolean(false);

        doAnswer(invocationOnMock -> {
            hasPrepared.set(true);
            return null;
        }).when(outputCollector).ack(mockedTuple);


        executor.execute(mockedTuple);
        verify(mockedBolt, times(1)).prePrepare(12345L);
        verify(state, times(1)).prepareCommit(12345L);
        assertTrue(hasPrepared.get());


        /* commit: the method bolt.preCommit() should be invoked, with the txid specified */
        when(mockedTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(CheckPointState.Action.COMMIT);

        AtomicBoolean hasCommitted = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            hasCommitted.set(true);
            return null;
        }).when(outputCollector).ack(mockedTuple);

        executor.execute(mockedTuple);
        verify(mockedBolt, times(1)).preCommit(12345L);
        verify(state, times(1)).commit(12345L);
        assertTrue(hasCommitted.get());
    }

    /* test of methods processCheckpoint and handleCheckpoint, wrt to the action ROLLBACK, through the invocation of execute() on checkpoint tuples */
    @Test
    public void testRollbackCheckpoint() {

        /*  Expected behavior: the methods bolt.preRollback() and state.rollback() are invoked, then the execution is acked to the OutputCollector */

        Tuple mockTuple = mock(Tuple.class);
        State state = mock(State.class);

        executor.prepare(topoConf, context, outputCollector, state);

        when(mockTuple.getSourceStreamId()).thenReturn(CHECKPOINT_STREAM_ID);
        when(mockTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(CheckPointState.Action.ROLLBACK);
        when(mockTuple.getValueByField(CHECKPOINT_FIELD_TXID)).thenReturn(54321L);

        AtomicBoolean hasRollback = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            hasRollback.set(true);
            return null;
        }).when(outputCollector).ack(mockTuple);

        executor.execute(mockTuple);
        verify(mockedBolt, times(1)).preRollback();
        verify(state, times(1)).rollback();
        assertTrue(hasRollback.get());
    }
}
