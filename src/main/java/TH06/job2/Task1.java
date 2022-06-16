package TH06.job2;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import task.CustomerPayment;
import task.OrderPayment;
import task.OrderPaymentGenerator;
import task.SimpleStringGenerator;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class Task1 {
    public static void main(String[] args) throws Exception {
        // the source data stream
        StreamExecutionEnvironment env=StreamContextEnvironment.getExecutionEnvironment();
        DataStream<String> messageStream = env.addSource(new OrderPaymentGenerator());
        messageStream.print();
        messageStream.addSink(new FlinkKafkaProducer<String>("10.1.12.183:9092",
                "OrderPayment_188702",
                new SimpleStringSchema()));
        env.execute("Flink producer");
    }
}