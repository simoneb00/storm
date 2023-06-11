package org.apache.storm.spout;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.storm.spout.CheckpointSpout.*;

public class TupleCollectorBolt extends BaseRichBolt implements IRichBolt {
    private OutputCollector collector;
    private static List<Tuple> emittedTuples = new ArrayList<>();

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void execute(Tuple tuple) {
        CheckPointState.Action action = (CheckPointState.Action) tuple.getValueByField(CHECKPOINT_FIELD_ACTION);
        emittedTuples.add(tuple);;
        collector.ack(tuple);
    }

    public List<Tuple> getEmittedTuples() {
        return emittedTuples;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    public List<CheckPointState.Action> getTrackedActions() {
        List<CheckPointState.Action> actions = new ArrayList<>();

        for (Tuple tuple : emittedTuples) {
            actions.add((CheckPointState.Action) tuple.getValueByField(CHECKPOINT_FIELD_ACTION));
        }

        return actions;
    }

}
