package TH06.job1;

import task.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Kafka_Flink_Consumer {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "group1");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer <>("OrderPayment_188702",  new SimpleStringSchema(), properties);
        DataStream<String> eachKafkaData = env.addSource(kafkaConsumer);
        /*DataStream<String> stringInputStream = env.addSource(kafkaConsumer);
        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer());*/
        eachKafkaData.print();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

        // execute program
        env.execute("Flink consumer");
    }
}
