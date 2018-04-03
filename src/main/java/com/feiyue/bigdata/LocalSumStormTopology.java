package com.feiyue.bigdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class LocalSumStormTopology {
    static class DataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector collector;


        /**
         * 初始化方法，只被调用一次
         * @param map
         * @param topologyContext
         * @param spoutOutputCollector
         */
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;

        }

        int number = 0;

        public void nextTuple() {
            this.collector.emit(new Values(++number));

            System.out.println("Spout:" + number);

            Utils.sleep(1000);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("number"));
        }
    }

    static class SumBolt extends BaseRichBolt{
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        int sum = 0;

        public void execute(Tuple input) {
            Integer value = input.getIntegerByField("number");
            sum += value;
            System.out.println("Bolt: sum= [" + sum + "]");
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SumBolt",new SumBolt()).shuffleGrouping("DataSourceSpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology",new Config(),builder.createTopology());

    }
}
