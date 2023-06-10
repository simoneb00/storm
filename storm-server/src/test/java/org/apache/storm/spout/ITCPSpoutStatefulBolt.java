/* this testing class is located in the storm-server module to allow the usage of the class LocalCluster (to avoid cyclic dependencies with the module storm-client) */
package org.apache.storm.spout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_FIELD_ACTION;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

/**
 * This class tests the interaction between {@link org.apache.storm.spout.CheckpointSpout} and  {@link org.apache.storm.topology.StatefulBoltExecutor}.
 * The interaction takes place in an ad hoc topology, In particular, we have a standard topology that connects the CheckpointSpout to a custom bolt
 * ({@link TupleCollectorBolt}), that has the only responsibility to forward the tuples received from the spout to the BaseStatefulBoltExecutor. We test that
 * the tuples produced by the spout are correctly received and processed by the stateful bolt, and forwarded and processed by a mocked bolt connected to the
 * stateful bolt. The necessity of the intermediate custom bolt ({@link TupleCollectorBolt}) derives from an implementation detail of {@link LocalCluster},
 * that is used as support for the execution of the topology.
 */
public class ITCPSpoutStatefulBolt {
    private LocalCluster localCluster;
    @Test
    public void testInteraction() {

        try {

            MockitoAnnotations.openMocks(this);

            localCluster = new LocalCluster();

            TopologyBuilder builder = new TopologyBuilder();

            TupleCollectorBolt bolt = new TupleCollectorBolt();

            DummyBolt dummyBolt = new DummyBolt();
            StatefulBoltExecutor statefulBoltExecutor = new StatefulBoltExecutor<>(dummyBolt);
            CheckpointSpout spout = new CheckpointSpout();


            builder.setSpout("spout", spout);
            builder.setBolt("stateful_bolt", statefulBoltExecutor).shuffleGrouping("spout", "$checkpoint");

            Config config = new Config();
            config.setDebug(true);

            localCluster.submitTopology("topology", config, builder.createTopology());

            try {
                Thread.sleep(6000);

            } catch (InterruptedException e) {
                Assertions.fail("Cluster abnormally interrupted");
            }

            localCluster.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }
}
