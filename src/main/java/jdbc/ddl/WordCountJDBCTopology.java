package jdbc.ddl;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
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

public class WordCountJDBCTopology {
    public static class DataSource extends BaseRichSpout{
        private SpoutOutputCollector Collector;

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector Collector) {
            this.Collector = Collector;
        }
        public static final String[] words = new String[]{"apple","orange","pineapple","banana","pie"};

        public void nextTuple() {
            Random random = new Random();
            String word = words[random.nextInt(words.length)];

            this.Collector.emit(new Values(word));
            System.out.println("emit"+word);
            Utils.sleep(1000);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("lines"));

        }
    }

    public static class SplitBolt extends BaseRichBolt{

        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector = collector;

        }

        public void execute(Tuple input) {
            String word = input.getStringByField("lines");

            this.collector.emit(new Values(word));


        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));

        }
    }

    public static class CountBolt extends BaseRichBolt{

        private OutputCollector collector;
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector = collector;

        }
        Map<String,Integer> map = new HashMap<String, Integer>();

        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (count==null){
                count = 0;
            }else{
                count ++;
            }
            map.put(word,count);
            System.out.println(word+"  "+count);



            this.collector.emit(new Values(word,map.get(word)));

            }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","word_count"));

        }
    }



    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSource",new DataSource());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSource");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://120.79.54.138/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "wordcount";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);
        builder.setBolt("JdbcInserBolt",userPersistanceBolt).shuffleGrouping("CountBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountTopology", new Config(),builder.createTopology());
    }
}