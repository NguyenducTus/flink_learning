package TH07.resources;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import task.CustomerPayment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SinkOrderPayment implements FlatMapFunction<CustomerPayment, String> {
    private transient ValueState<Tuple2<Integer, Float>> sum;
    private IMap<Tuple2, HazelcastJsonValue> map;
    public SinkOrderPayment(String s) {
    }

    @Override
    public void flatMap(CustomerPayment customerPayment, Collector<String> collector) throws Exception {
/*
        map.put(new Tuple2<>(customerPayment.customerId, customerPayment.payableType), new HazelcastJsonValue());
*/      /*Tuple2<Integer, Float> currentSum = sum.value();
        for (Map.Entry<Tuple2, HazelcastJsonValue> entry : map.entrySet()) {
            Integer keyCustomerId = (Integer) entry.getKey().f0;
            Integer keyPayableType = (Integer) entry.getKey().f1;
            if(keyCustomerId==customerPayment.customerId
                    && keyPayableType==customerPayment.payableType
                    &&(currentSum.f0==0))
            {
                HazelcastJsonValue jsonString = entry.getValue();
                ObjectMapper mapper = new ObjectMapper();
                C_CUSTOMER_PAYMENTNUMBER c = mapper.readValue(jsonString.toString(), C_CUSTOMER_PAYMENTNUMBER.class);
                currentSum.f0=c.USETIME;
                currentSum.f1= c.TOTALAMOUNT;
            }
        }
        ObjectMapper objectMapper=new ObjectMapper();
        List<String> strList= Arrays.asList("["+objectMapper.writeValueAsString(customerPayment)+"]");
        for (String item:strList) {
            collector.collect(item);
        }*/
    }
}
