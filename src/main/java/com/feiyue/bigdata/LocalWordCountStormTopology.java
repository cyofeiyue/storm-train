package com.feiyue.bigdata;

import org.apache.commons.io.FileUtils;
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

import java.io.IOException;
import java.util.*;
import java.io.File;

public class LocalWordCountStormTopology {


    public static class DataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector collector;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;

        }

        public void nextTuple() {
            Collection<File> files = FileUtils.listFiles(new File("/Users/nisaisai/IdeaProjects/hadoop-shizhan/data/storm-data"),new String[]{"txt"},true);
            for (File file : files){
                try {
                    List<String> lines = FileUtils.readLines(file);
                    for (String line : lines){
                        this.collector.emit(new Values(line));
                    }

                    FileUtils.moveFile(file,new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));

        }

    }

    public static class SplitBolt extends BaseRichBolt{
        OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");
            for (String word : words){
                this.collector.emit(new Values(word));
            }


        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));

        }
    }

    public static class CountBolt extends BaseRichBolt{

        Map<String,Integer> map = new HashMap<String, Integer>();
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (count == null){
                count = 0;
            }
            count++;
            map.put(word,count);

            System.out.println("-----------------");
            Set<Map.Entry<String,Integer>> entrySet = map.entrySet();
            for (Map.Entry<String,Integer> entry : entrySet){
                System.out.println(entry);
            }


        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountStormTopology",new Config(),builder.createTopology());
    }
}
