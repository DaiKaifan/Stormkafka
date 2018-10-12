package cn.hotmap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 接受kafka的数据进行开发
 */
public class LogProcessBolt extends BaseRichBolt{

    private  OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

    }

    public void execute(Tuple input) {
        try {
            byte[] binaryByField = input.getBinaryByField("bytes"); //kafka_storm field固定为bytes
            String value = new String(binaryByField);
            /**
             * 13250455817	116.1706900000,23.2952800000
             * 解析日志信息
             */
            String[] data =value.split("\t");
            String[] temp = data[0].split(",");
            String longitude = temp[0]; //经度
            String latitude = temp[1]; //纬度
            String in_location =data[1];
            this.collector.emit(new Values(Double.parseDouble(longitude),Double.parseDouble(latitude),in_location));

            this.collector.ack(input);
        }catch (Exception e) {
            this.collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("longitude","latitude","out_location"));

    }
}