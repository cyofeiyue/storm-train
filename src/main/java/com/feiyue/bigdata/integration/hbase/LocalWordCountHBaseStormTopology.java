package com.feiyue.bigdata.integration.hbase;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class LocalWordCountHBaseStormTopology {


    /**
     * DataSourceSpout
     * 读取文件的每一行并发出line
     */
    static class RandomSentenceDataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;

        }


        //随机数组
        public static String[] sentences = new String[]{"the cow jumped over the moon",
                "green an apple a day keeps the doctor away ",
                "blue four socre seven years ago",
                "show white and rhe haha zheshi ",
                "three am at two width natrue"};

        @Override
        public void nextTuple() {
            String sentence = sentences[new Random().nextInt(sentences.length)];

            this.collector.emit(new Values(sentence));
            Utils.sleep(1000);
            System.out.println("---------------sentence:" + sentence);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));

        }
    }

    /**
     * SplitBolt
     * 对于每一行内容进行分割，发出每一个单词
     *
     */
    static class SplitSentenceBolt extends BaseRichBolt{
       private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String sentence = input.getString(0);
            String[] words = sentence.split(" ");
            for(String word : words){
                word = word.trim();
                if(!word.isEmpty()){
                    this.collector.emit(new Values(word));
                    System.out.println("SplitSentenceBolt:" + word);
                }
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }


    static class CountBolt extends BaseRichBolt{

        private Map<String,Integer> countersMap = new HashMap<String, Integer>();
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
           String word =  input.getString(0);
           if (!countersMap.containsKey(word)){
               countersMap.put(word,1);
           }else{
               Integer c = countersMap.get(word) + 1;
               countersMap.put(word,c);
           }
            this.collector.emit(new Values(word, String.valueOf(countersMap.get(word))));

            System.out.println("word:" + word + "count:"+countersMap.get(word));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new RandomSentenceDataSourceSpout());
        builder.setBolt("SplitBolt",new SplitSentenceBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).fieldsGrouping("SplitBolt",new Fields("word"));

        Config config = new Config();
        Map<String,Object> hbaseConf = new HashMap<String, Object>();
        hbaseConf.put("hbase.rootdir","hdfs://192.168.21.183:8020/hbase");
        hbaseConf.put("hbase.zookeeper.quorum","192.168.21.183:2181");
        config.put("hbase.conf",hbaseConf);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");

        HBaseBolt hBaseBolt = new HBaseBolt("wc",mapper).withConfigKey("hbase.conf");
        builder.setBolt("HBaseBolt",hBaseBolt).shuffleGrouping("CountBolt");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountHBaseStormTopology",config,builder.createTopology());
    }

}
