package task;

import java.util.Random;

public class CustomerPayment {
    public String customerPaymentId;
    public int customerId;
    public int brandId;
    public int payableType;
    public int transactionTypeId;
    public int useTime;
    public float totalAmount;

    public CustomerPayment(int customerId, int brandId, int payableType, int transactionTypeId, int useTime, float totalAmount){
        this.customerPaymentId=CustomerPaymentId();
        this.customerId=customerId;
        this.brandId=brandId;
        this.payableType=payableType;
        this.transactionTypeId=transactionTypeId;
        this.useTime=useTime;
        this.totalAmount=totalAmount;
    }

    private String CustomerPaymentId(){
        Random rnd=new Random();
        Integer result=rnd.nextInt(2000000000);
        return result.toString();
    }
}
