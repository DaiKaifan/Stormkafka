package cn.hotmap;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;


public class StormKafkaTopo {
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        //kafka topic
        String topic = "ingoods";
        //Kafaka存储数据的topic名称 /project_topic
        String zkRoot = "/stormkafka";
        String id = "ingoods";
        //定义kafka集群使用的zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("120.77.82.157:2181,120.79.54.138:2181,111.230.135.163:2181");
        //数据源spout配置信息
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,zkRoot,id);
        //设置每次storm只处理最新的数据
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID,kafkaSpout);
        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID,new LogProcessBolt()).shuffleGrouping(SPOUT_ID);


        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://120.77.82.157/storm");
        hikariConfigMap.put("dataSource.user","hadoop");
        hikariConfigMap.put("dataSource.password","hadoop");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "out_tude"; //收货
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);
        builder.setBolt("JdbcInserBolt",userPersistanceBolt).shuffleGrouping(BOLT_ID);


        try {
            StormSubmitter.submitTopology("StormKafkaOut",new Config(),builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}