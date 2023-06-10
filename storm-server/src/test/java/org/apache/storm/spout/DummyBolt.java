package org.apache.storm.spout;

import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IStatefulBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class DummyBolt implements IStatefulBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void initState(State state) {

    }

    @Override
    public void preCommit(long txid) {

    }

    @Override
    public void prePrepare(long txid) {

    }

    @Override
    public void preRollback() {

    }
}
