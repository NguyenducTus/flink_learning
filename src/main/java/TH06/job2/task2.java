package TH06.job2;
import TH06.*;
import task.SimpleStringSchema;
import task.CustomerPayment;
import task.OrderPayment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Properties;
public class task2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "group1");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer <>("OrderPayment_188702",  new SimpleStringSchema(), properties);
        DataStream<String> strInputStream = env.addSource(consumer);
        DataStream<OrderPayment> message=strInputStream.flatMap(new CustomerTokenizer());
        DataStream<CustomerPayment>
                groupMessage=message.keyBy(new KeyByDescription())
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .process(new KeyProcess());
        DataStream<String> strgroupMessage=groupMessage.flatMap(new CustomerPaymentToString());
        strgroupMessage.addSink(new FlinkKafkaProducer<String>("10.1.12.183:9092",
                "CustomerPayment_188702",
                new SimpleStringSchema()));

        /*groupMessage.addSink(new FlinkKafkaProducer<CustomerPayment>("10.1.12.183:9092",
                "CustomerPayment_Chien188210",
                new SimpleStringSchema()));*/
        env.execute();
    }

}
