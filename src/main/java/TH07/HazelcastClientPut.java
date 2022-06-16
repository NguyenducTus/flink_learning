package TH07;


import TH06.job2.CustomerTokenizer;
import TH06.job2.KeyByDescription;
import TH07.resources.ProcessSinkOrderPayment;
import task.SimpleStringSchema;
import task.CustomerPayment;
import task.OrderPayment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class HazelcastClientPut {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "group1");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer <>("OrderPayment_188702",  new SimpleStringSchema(), properties);
        DataStream<String> strInputStream = env.addSource(consumer);
        DataStream<OrderPayment> message=strInputStream.flatMap(new CustomerTokenizer());
        DataStream<CustomerPayment>
                groupMessage= message.keyBy(new KeyByDescription())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(ProcessingTimeTrigger.create())
                .process(new ProcessSinkOrderPayment("10.1.6.216:5701"));
        env.execute();
    }
}
