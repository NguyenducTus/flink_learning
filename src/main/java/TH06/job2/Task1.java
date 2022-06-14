package TH06.job2;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import task.CustomerPayment;
import task.OrderPayment;

import java.util.Properties;

public class Task1 {
    public static void main(String[] args) throws Exception {
        // the source data stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "TH06");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("CustomerPayment_188702", new SimpleStringSchema(), properties));

        DataStream<OrderPayment> od = stream.flatMap(new Tokenizer());

        DataStream<CustomerPayment> groupMessage =
                od.keyBy(new KeyByDescription())
                        .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                        .process(new ProcessWindow());
        groupMessage.print();
        env.execute();
    }
}