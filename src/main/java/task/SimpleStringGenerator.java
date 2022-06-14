package task;

import task.OrderPayment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleStringGenerator implements SourceFunction<String> {
    private static final long serialVersionUID = 2174904787118597072L;
    boolean running = true;
    long i = 0;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(running) {
            OrderPayment message = new OrderPayment();
            ctx.collect(message.toString());
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
