package task;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class OrderPaymentGenerator implements SourceFunction<String> {

    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private static final int NUMBER_OF_MESSAGE = 10;
    private static final int NUMBER_OF_CUSTOMER = 1000;
    private static final int BRAND_ID = 5;
    private static final int NUMBER_OF_TOTAL = 10000;
    private volatile boolean running = true;

    public String Id() {
        Random rnd = new Random();
        String id= "ID"+rnd.nextInt( NUMBER_OF_MESSAGE-1 )+1;
        return id;
    }
    public int  CustomerID(){
          Random rnd = new Random();
          return rnd.nextInt( NUMBER_OF_CUSTOMER-1 )+1;
    }
    public int  BrandID(){
        Random rnd = new Random();
        return rnd.nextInt( BRAND_ID-1 )+1;
    }
    public String  SaleOrderID(){
        Random rnd = new Random();
        String saleID= "DH"+rnd.nextInt( NUMBER_OF_CUSTOMER-1 )+1;
        return saleID;
    }
    public int PayableType()
    {
        Random rnd = new Random();
        return  rnd.nextInt( 4 )+1;
    }
    public int TransactionTypeID()
    {
        if(this.PayableType()==1)
            return 1;
        else
            return 0;

    }
    public int Total(){
        Random rnd = new Random();
        return  rnd.nextInt( NUMBER_OF_TOTAL-1 )+1;
    }




    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int count = 0;

        while (running&&count<NUMBER_OF_MESSAGE) {

            for (int i = 1; i <= BATCH_SIZE; i++) {
                OrderPayment oder = new OrderPayment();
                sourceContext.collect(oder.toString());
                System.out.println(oder.toString());
            }
            // prepare for the next batch
            count += BATCH_SIZE;
            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }

        sourceContext.close();
    }

    @Override
    public void cancel() {
        running=false;
    }
}
