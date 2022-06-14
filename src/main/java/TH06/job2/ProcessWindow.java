package TH06.job2;

import org.apache.flink.api.java.tuple.Tuple2;
import task.CustomerPayment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import task.OrderPayment;

public class ProcessWindow extends ProcessWindowFunction<OrderPayment, CustomerPayment, org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>, TimeWindow> {
    @Override
    public void process(org.apache.flink.api.java.tuple.Tuple2<Integer, Integer> key,
                        ProcessWindowFunction<OrderPayment, CustomerPayment, Tuple2<Integer, Integer>, TimeWindow>.Context context,
                        Iterable<OrderPayment> elements,
                        Collector<CustomerPayment> collector) throws Exception {
        float totalAmount = 0;
        int numPay = 0;
        for (OrderPayment item : elements) {
            totalAmount += item.TOTALAMOUNT;
            numPay++;
        }
        collector.collect(new CustomerPayment(key.f0, elements.iterator().next().BRANDID, key.f1, elements.iterator().next().TRANSACTIONTYPEID, numPay, totalAmount));

    }
}