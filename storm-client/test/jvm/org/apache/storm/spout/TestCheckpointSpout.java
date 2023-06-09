package org.apache.storm.spout;

import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.storm.utils.TestingUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 *  Unit test for the class {@link CheckpointSpout}.
 */
public class TestCheckpointSpout {

    private CheckpointSpout spout;
    private SpoutOutputCollector outputCollector;
    private KeyValueState<String, CheckPointState> state;

    @BeforeEach
    public void setUp() {
        spout = new CheckpointSpout();
        outputCollector = getSpoutOutputCollector(VALID);
        state = new InMemoryKeyValueState<>();
    }

    private static Stream<Arguments> openParams() {
        return Stream.of(
                Arguments.of(getTopoConf(VALID), getContext(VALID), getSpoutOutputCollector(VALID), false),
                //Arguments.of(getTopoConf(INVALID), getContext(VALID), getSpoutOutputCollector(VALID), true),  TODO
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getSpoutOutputCollector(VALID), true),
                //Arguments.of(getTopoConf(VALID), getContext(VALID), getSpoutOutputCollector(INVALID), true),  TODO
                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getSpoutOutputCollector(VALID), true),
                Arguments.of(getTopoConf(VALID), getContext(INVALID), getSpoutOutputCollector(INVALID), true),
                //Arguments.of(getTopoConf(INVALID), getContext(VALID), getSpoutOutputCollector(INVALID), true),   TODO
                Arguments.of(getTopoConf(INVALID), getContext(INVALID), getSpoutOutputCollector(INVALID), true)
        );
    }

    @ParameterizedTest
    @MethodSource("openParams")
    public void testOpenSpout(Map<String, Object> topoConf, TopologyContext context, SpoutOutputCollector collector, boolean exceptionExpected) {
        try {
            spout.open(topoConf, context, collector);
            assertFalse(exceptionExpected);
        } catch (Exception e) {
            assertTrue(exceptionExpected);
        }
    }

    private static Stream<Arguments> testIsCheckpointTupleParams() {
        return Stream.of(
                Arguments.of(true),
                Arguments.of(false)
        );
    }

    @ParameterizedTest
    @MethodSource("testIsCheckpointTupleParams")
    public void testIsCheckpointTuple(boolean isCheckpointTuple) {

        Tuple mockTuple = mock(Tuple.class);

        if (isCheckpointTuple) {
            when(mockTuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);
        }

        assertEquals(isCheckpointTuple, CheckpointSpout.isCheckpoint(mockTuple));
    }

    /**
     * From the javadoc of the class {@link CheckPointState}:
     * <p><i>"During recovery, if a previous transaction is in PREPARING state, it is rolled back since all bolts in the topology might not have
     *  * prepared (saved) the data for commit."</i></p>
     *  So we expect that, in recovery mode (i.e. opening the spout without acking anything), and trying to handle a PREPARE tuple, the emitted state will be ROLLBACK,
     *  and the next state, according to the state transitions below, will be COMMITTED (relative to transaction with id = txid-1).
     *
     *  <pre>
     *                  ROLLBACK(tx2)
     *                 <-------------                  PREPARE(tx2)                     COMMIT(tx2)
     *   COMMITTED(tx1)-------------> PREPARING(tx2) --------------> COMMITTING(tx2) -----------------> COMMITTED (tx2)
     *
     *  </pre>
     */
    @Test
    public void testPrepare() {
        CheckPointState checkPointState = new CheckPointState(1L, CheckPointState.State.PREPARING);
        state.put("__state", checkPointState);
        spout.open(getContext(VALID), outputCollector, 1000, state);

        AtomicBoolean checkRollback = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            checkRollback.set(true);
            return null;
        }).when(outputCollector).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.ROLLBACK), 1L);

        spout.nextTuple();

        assertTrue(checkRollback.get());

        spout.ack(1L);   // acking the previous transaction -> the next state should become COMMITTED for the transaction with txid = 0,
                               // so, according to CheckPointState, the next action should be INITSTATE, with txid = 0

        AtomicBoolean checkInitState = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            checkInitState.set(true);
            return null;
        }).when(outputCollector).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(0L, CheckPointState.Action.INITSTATE), 0L);

        spout.nextTuple();

        assertTrue(checkInitState.get());

        spout.ack(0L);

        /* at this point, we should be able to emit the action PREPARE for the transaction with txid = 1 */

        AtomicBoolean checkPrepare = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            checkPrepare.set(true);
            return null;
        }).when(outputCollector).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.PREPARE), 1L);

        spout.nextTuple();

        assertTrue(checkPrepare.get());

        spout.ack(1L);

        /* now, the next state should be COMMITTING (txid = 1), so the next action should be COMMIT (txid = 1) */

        AtomicBoolean checkCommit = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            checkCommit.set(true);
            return null;
        }).when(outputCollector).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.COMMIT), 1L);

        spout.nextTuple();

        assertTrue(checkCommit.get());
    }

    /*
     *  here we're testing the following transition:
     *
     *                         COMMIT(txid)
     *      COMMITTING(txid) -----------------> COMMITTED (txid)
     */
    @Test
    public void testCommitting() {
        CheckPointState checkPointState = new CheckPointState(1L, CheckPointState.State.COMMITTING);
        state.put("__state", checkPointState);
        spout.open(getContext(VALID), outputCollector, 1000, state);

        AtomicBoolean checkCommit = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            checkCommit.set(true);
            return null;
        }).when(outputCollector).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.COMMIT), 1L);

        spout.nextTuple();

        assertTrue(checkCommit.get());
    }

    /*
     *  According to the state transitions, after the COMMITTED action with id = txid is acked, the next state is PREPARING for the subsequent transaction,
     *  so the next action is PREPARE for txid = txid + 1.
     *  Checking the following transition:
     *
     *                                                      PREPARE(txid+1)
     *      COMMITTED(txid)-------------> PREPARING(txid+1) -------------->
     */
    @Test
    public void testCommitted() {
        CheckPointState checkPointState = new CheckPointState(1L, CheckPointState.State.COMMITTED);
        state.put("__state", checkPointState);
        spout.open(getContext(VALID), outputCollector, 1000, state);

        spout.nextTuple();

        // here the emitted action is INITSTATE (txid = 1), already tested in testPrepare()

        spout.ack(1L);

        /* verify that the next action is PREPARING for txid = 2L */

        AtomicBoolean checkPreparingNext = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            checkPreparingNext.set(true);
            return null;
        }).when(outputCollector).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(2L, CheckPointState.Action.PREPARE), 2L);

        spout.nextTuple();

        assertTrue(checkPreparingNext.get());
    }


    /*  DOMAIN PARTITIONING:
     *  the parameter Object msgId represents the ID of the transaction to ack.
     *  Equivalence classes: {invalid_instance}, {valid_instance}, {null}, where:
     *      - a valid instance is a long
     *      - an invalid instance is another Object, that is not a long

     *          boundary value                                         expected result
     * -------------------------------------------------------------------------------------------------------------------
     *     "string" (invalid_instance),  |      exception (invalid id)
     *     1L (valid_instance),          |      if it exists a non-acked transaction with txid = 1, success; oth. nothing
     *     null                          |      exception (invalid id)
     *
     *
     */
    private static Stream<Arguments> ackParams() {
        return Stream.of(
                Arguments.of("string", true),
                Arguments.of(1L, false),
                Arguments.of(null, true)
        );
    }

    @ParameterizedTest
    @MethodSource("ackParams")
    public void testAck(Object msgId, boolean exceptionExpected) {

        spout.open(getTopoConf(VALID), getContext(VALID), getSpoutOutputCollector(VALID));

        try {

            /* first, we assume there's no non-acked pending transaction with txid = msgId
            *  (this is intended to test, subsequently, the ack method on an actual non-acked pending tx,
            *   but it can be possible that a non-acked transaction with txid = msgId exists: in that case,
            *   the transaction is acked now, and then will be re-acked) */
            spout.ack(msgId);

            /* now we create a transaction with txid = msgId, and we try to ack it */
            spout.close();

            CheckPointState checkPointState = new CheckPointState((long) msgId, CheckPointState.State.COMMITTING);
            state.put("__state", checkPointState);
            spout.open(getContext(VALID), getSpoutOutputCollector(VALID), 1000, state);

            spout.ack(msgId);

            assertFalse(exceptionExpected);

        } catch (ClassCastException | NullPointerException e) {     // we expect either a ClassCastException (it is not possible to cast msgId to a long)
            assertTrue(exceptionExpected);                          // or a NullPointerException (msgId = null)
        }
    }
}
