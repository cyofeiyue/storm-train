package com.feiyue.bigdata.integration.jdbc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.collect.Maps;
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


public class LocalWordCountJDBCStormTopology {


    /**
     * DataSourceSpout
     * 读取文件的每一行并发出line
     */
    static class DataSourceSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;

        }

//        @Override
//        public void nextTuple() {
//            Collection<File> files = FileUtils.listFiles(new File("/Users/nisaisai/IdeaProjects/hadoop-shizhan/data/storm-data"),new String[]{"txt"},true);
//            for (File file : files){
//                try {
//                    List<String> lines = FileUtils.readLines(file);
//                    for(String line : lines){
//                        //发出每一个文件的每一行
//                        this.collector.emit(new Values(line));
//                    }
//
//                    //一个文件的内容全部发送完以后，重命名文件名
//                    //FileUtils.moveFile(file,new File(file.getAbsolutePath() + System.currentTimeMillis()));
//                    Utils.sleep(1000);
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }

        //随机数组
        public static String[] words = new String[]{"apple","green","blue","aaa","bbb","asdf","orange"};

        @Override
        public void nextTuple() {
            String word = words[new Random().nextInt(words.length)];
            this.collector.emit(new Values(word));
            Utils.sleep(1000);
            System.out.println("---word---:" + word);
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
       private OutputCollector collector;
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

        private Map<String,Integer> map = new HashMap<String, Integer>();
        private OutputCollector collector;

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



    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");


        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://192.168.21.174/test");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "word_count";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("userPersistanceBolt",userPersistanceBolt).shuffleGrouping("CountBolt");
        //Or
        //JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
        //        .withInsertQuery("insert into user values (?,?)")
         //       .withQueryTimeoutSecs(30);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountReidsStormTopology",new Config(),builder.createTopology());
    }

}
