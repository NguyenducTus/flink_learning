package TH06.job2;
import task.OrderPayment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyByDescription implements KeySelector<OrderPayment, Tuple2<Integer, Integer>> {
    @Override
    public Tuple2<Integer, Integer> getKey(OrderPayment orderPayment) throws Exception {
        Tuple2<Integer, Integer> result=new Tuple2<>();
        result.f0= orderPayment.CUSTOMERID;
        result.f1=orderPayment.PAYABLETYPE;
        return result;
    }
}