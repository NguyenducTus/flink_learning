package task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Tokenizer implements FlatMapFunction<String, OrderPayment> {
    @Override
    public void flatMap(String value, Collector<OrderPayment> out) {
        ObjectMapper mapper = new ObjectMapper();
        try {

            List<OrderPayment> serviceModels = Arrays.asList(mapper.readValue(value, OrderPayment[].class));

            for (OrderPayment item: serviceModels) {
                // convert string to my object
                out.collect(item);
            }

        } catch (IOException e) {

            e.printStackTrace();

        }

    }
}
