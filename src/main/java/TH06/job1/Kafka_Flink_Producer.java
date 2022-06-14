package TH06.job1;

import task.SimpleStringGenerator;
import task.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Kafka_Flink_Producer {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        // add a simple source which is writing some strings
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

        // write stream to Kafka
        messageStream.addSink(new FlinkKafkaProducer<>("10.1.12.183:9092",
                "OrderPayment_188702",
                new SimpleStringSchema()));


        // execute program
        env.execute("Flink producer");
    }
    //https://git.thegioididong.com/outsource/flink-learning/-/blob/main/src/main/java/com/task/DataGenerator.java
    /*public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 0;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(running) {
                ctx.collect("element-"+ (i++));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    public static class SimpleStringSchema implements DeserializationSchema<String>, SerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        public SimpleStringSchema() {
        }

        public String deserialize(byte[] message) {
            return new String(message);
        }

        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        public byte[] serialize(String element) {
            return element.getBytes();
        }

        public TypeInformation<String> getProducedType() {
            return TypeExtractor.getForClass(String.class);
        }
    }*/
}
