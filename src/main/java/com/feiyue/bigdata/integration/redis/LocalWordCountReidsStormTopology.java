package com.feiyue.bigdata.integration.redis;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LocalWordCountReidsStormTopology {


    /**
     * DataSourceSpout
     * 读取文件的每一行并发出line
     */
    static class DataSourceSpout extends BaseRichSpout{
        SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;

        }

        @Override
        public void nextTuple() {
            Collection<File> files = FileUtils.listFiles(new File("/Users/nisaisai/IdeaProjects/hadoop-shizhan/data/storm-data"),new String[]{"txt"},true);
            for (File file : files){
                try {
                    List<String> lines = FileUtils.readLines(file);
                    for(String line : lines){
                        //发出每一个文件的每一行
                        this.collector.emit(new Values(line));
                    }

                    //一个文件的内容全部发送完以后，重命名文件名
                    //FileUtils.moveFile(file,new File(file.getAbsolutePath() + System.currentTimeMillis()));
                    Utils.sleep(1000);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));

        }
    }

    /**
     * SplitBolt
     * 对于每一行内容进行分割，发出每一个单词
     *
     */
    static class SplitBolt extends BaseRichBolt{
        OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");
            for(String word : words){
                this.collector.emit(new Values(word));
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 对每一个接受到的单词进行计数，并将<word,count>对发送出去
     */
    static class CountBolt extends BaseRichBolt{

        Map<String,Integer> map = new HashMap<String, Integer>();
        OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");

            Integer count = map.get(word);
            if(count == null) {
                count = 0;
            }
            count++;
            //count --> 1

            map.put(word,count);

            this.collector.emit(new Values(word, map.get(word)));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));

        }
    }

    public static class WordCountStoreMapper implements RedisStoreMapper {

        private RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return tuple.getIntegerByField("count") + " ";
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("192.168.21.170").setPort(6379).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

        builder.setBolt("RedisStoreBolt",storeBolt).shuffleGrouping("CountBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountReidsStormTopology",new Config(),builder.createTopology());
    }

}
