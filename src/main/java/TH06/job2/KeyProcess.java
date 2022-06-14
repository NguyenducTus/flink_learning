package TH06.job2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import task.OrderPayment;
import task.CustomerPayment;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class KeyProcess extends ProcessWindowFunction<OrderPayment, CustomerPayment, org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>, TimeWindow> {
    private transient ValueState<Float> sum;
    private transient ValueState<Integer> dem;
    private transient  ValueState<Long> state;
    private transient ValueState<CustomerPayment> cus;

    @Override
    public void open(Configuration parameters) throws Exception {
        sum = getRuntimeContext().getState(new ValueStateDescriptor<>("myTotal", Float.class));
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Long.class));
        cus = getRuntimeContext().getState(new ValueStateDescriptor<>("myCus", CustomerPayment.class));
    }

    @Override
    public void process(org.apache.flink.api.java.tuple.Tuple2<Integer, Integer> key,
                        ProcessWindowFunction<OrderPayment, CustomerPayment, Tuple2<Integer, Integer>, TimeWindow>.Context context,
                        Iterable<OrderPayment> elements,
                        Collector<CustomerPayment> collector) throws Exception {
        float totalAmount=0;
        int numPay=0;
        for (OrderPayment item: elements) {
            totalAmount+=item.TOTALAMOUNT;
            numPay++;
        }
        collector.collect(new CustomerPayment(key.f0, elements.iterator().next().BRANDID, key.f1, elements.iterator().next().TRANSACTIONTYPEID, numPay, totalAmount));

    }
/*
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<CustomerPayment, OrderPayment,Tuple2<Float,Integer>>.OnTimerContext ctx,
                        Collector<Tuple2<String,Float>> out) throws Exception {
        // get the state for the key that scheduled the timer
        Long result = state.value();

        // check if this is an outdated timer or the latest timer
        System.out.println("result "+result+" timestamp "+timestamp+
                " id"+cus.value().customerPaymentId+" sum"+sum.value());
        System.out.println();
        if (timestamp == result + 10000) {
            // emit the state on timeout
            //out.collect(new Tuple2<String,Float>(cus.value().customerPaymentId,sum.value());
            System.out.println("OUT COLLECT "+cus.value().customerPaymentId+" - "+sum.value());

        }
    }*/
}
