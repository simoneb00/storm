/* this testing class is located in the storm-server module to allow the usage of the class LocalCluster (to avoid cyclic dependencies with the module storm-client) */
package org.apache.storm.spout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.StatefulBoltExecutor;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_FIELD_ACTION;
import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_STREAM_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * This class tests the interaction between {@link org.apache.storm.spout.CheckpointSpout} and  {@link org.apache.storm.topology.StatefulBoltExecutor}.
 * The interaction takes place in a topology executed on a {@link LocalCluster}, a local execution cluster designed for testing purposes. The goal of this test
 * is to ensure that:
 * 1) the CheckpointSpout produces tuples when executing in a topology ( testProduction() )
 * 2) the tuples emitted by the CheckpointSpout actually reach the StatefulBoltExecutor
 * 3) the tuples emitted by the CheckpointSpout, that corresponds to checkpoint actions, are correctly processed by the StatefulBoltExecutor on a {@link DummyBolt} ( testProcessing() )
 * For the test case 1), an ad hoc implementation of a bolt is used ({@link TupleCollectorBolt}), that has the only responsibility of receive and store tuples, the test is
 * successful if, at the end of the cluster's execution, the list of received tuples is not empty (i.e., the spout has produced at least one tuple).
 * The test case 2) is contained in the test case 3), so we can implement just the latter.
 */
public class ITCPSpoutStatefulBolt {
    private LocalCluster localCluster;
    private TopologyBuilder builder;

    @BeforeEach
    public void setUp() {
        try {
            localCluster = new LocalCluster();
            builder = new TopologyBuilder();
        } catch (Exception e) {
            Assertions.fail("No exception should be thrown during configuration");
        }
    }


    /**
     * This method tests that the CheckpointSpout produces tuples and emits them into the topology.
     * To test this we use a bolt that subscribes to the spout, on the stream "$checkpoint" (where tuples are emitted) and collects tuples.
     * In the end, we check that at least one tuple is produced.
     */
    @Test
    public void testProduction() {
        TupleCollectorBolt tupleCollectorBolt = new TupleCollectorBolt();   // this bolt will be used to track the tuples emitted by CheckpointSpout
        CheckpointSpout checkpointSpout = new CheckpointSpout();

        builder.setSpout("checkpoint-spout", checkpointSpout);
        builder.setBolt("tuple-collector", tupleCollectorBolt).shuffleGrouping("checkpoint-spout", CHECKPOINT_STREAM_ID);   // subscription to the spout, in particular to the stream "$checkpoint"

        try {

            localCluster.submitTopology("production-topology", new Config(), builder.createTopology());   // cluster creation and execution
            Thread.sleep(5000);    // we let the cluster execute for 5 seconds
            localCluster.shutdown();

        } catch (TException e) {
            Assertions.fail("Unexpected exception in cluster creation");
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected exception in cluster execution");
        }

        assertFalse(tupleCollectorBolt.getEmittedTuples().isEmpty());

        for (Tuple tuple : tupleCollectorBolt.getEmittedTuples()) {
            System.out.println(tuple.getValueByField(CHECKPOINT_FIELD_ACTION));
        }
    }

    /**
     * This method tests either the reachability of StatefulBoltExecutor from CheckpointSpout and the correct processing of tuples.
     * The topology looks like this:
     * <pre>
     *                       |------------------>  StatefulBoltExecutor
     *     CheckpointSpout --|
     *                       |------------------>  TupleCollectorBolt
     *
     * </pre>
     * The TupleCollectorBolt acts as a log of the emitted tuples, we must check that the same sequence of actions is executed on the
     * StatefulBoltExecutor: to do that, we consider a {@link DummyBolt}, that is the actual bolt on which StatefulBoltExecutor performs the
     * checkpoint operations; in particular, the test is considered successful if the sequence of actions tracked by TupleCollectorBolt is
     * executed on DummyBolt.
     */
    @Test
    public void testProcessing() {

        DummyBolt dummyBolt = new DummyBolt();
        TupleCollectorBolt tupleCollectorBolt = new TupleCollectorBolt();
        StatefulBoltExecutor statefulBoltExecutor = new StatefulBoltExecutor(dummyBolt);
        CheckpointSpout checkpointSpout = new CheckpointSpout();

        builder.setSpout("checkpoint-spout", checkpointSpout);
        builder.setBolt("stateful-bolt-executor", statefulBoltExecutor).shuffleGrouping("checkpoint-spout", CHECKPOINT_STREAM_ID);
        builder.setBolt("tuple-collector", tupleCollectorBolt).shuffleGrouping("checkpoint-spout", CHECKPOINT_STREAM_ID);

        try {

            localCluster.submitTopology("reachability-topology", new Config(), builder.createTopology());   // cluster creation and execution
            Thread.sleep(5000);    // we let the cluster execute for 5 seconds
            localCluster.shutdown();

        } catch (TException e) {
            Assertions.fail("Unexpected exception in cluster creation");
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected exception in cluster execution");
        }

        assertEquals(tupleCollectorBolt.getTrackedActions(), dummyBolt.getActions());
    }
}
