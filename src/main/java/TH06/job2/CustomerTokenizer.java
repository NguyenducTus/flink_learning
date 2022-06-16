package TH06.job2;

import task.OrderPayment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class CustomerTokenizer implements FlatMapFunction<String, OrderPayment> {
        @Override
        public void flatMap(String value, Collector<OrderPayment> collector) throws Exception {
            ObjectMapper objectMapper=new ObjectMapper();
            List<OrderPayment> orderPaymentList= Arrays.asList(objectMapper.readValue(value, OrderPayment[].class));
            for (OrderPayment item:orderPaymentList) {
                collector.collect(item);
            }
        }
    }