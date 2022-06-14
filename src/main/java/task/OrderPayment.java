package task;

public class OrderPayment {
    public OrderPayment(){
        OrderPaymentGenerator d = new OrderPaymentGenerator();
        this.ID= d.Id();
        this.CUSTOMERID=d.CustomerID();
        this.BRANDID=d.BrandID();
        this.SALEORDERID=d.SaleOrderID();
        this.PAYABLETYPE=d.PayableType();
        this.TRANSACTIONTYPEID= d.TransactionTypeID();
        this.TOTALAMOUNT=d.Total();


    }
    public OrderPayment(String ID, int CUSTOMERID, int BRANDID, String SALEORDERID, int PAYABLETYPE,
                        int TRANSACTIONTYPEID,//Cổng thanh toán trực tuyến(Lưu khi PAYMENTTYPE=1)
                        long PAYTIME,//Thời gian thanh toán
                        float TOTALAMOUNT)
    {
        this.ID= ID;
        this.CUSTOMERID= CUSTOMERID;
        this.BRANDID= BRANDID;
        this.SALEORDERID=SALEORDERID;
        this.PAYABLETYPE=PAYABLETYPE;
        this.TRANSACTIONTYPEID=TRANSACTIONTYPEID;
        this.PAYTIME=PAYTIME;
        this.TOTALAMOUNT=TOTALAMOUNT;
    }
    public String ID;
    public int CUSTOMERID;//Mã khách hàng
    public int BRANDID;//Site
    public String SALEORDERID;//mã SO
    public int PAYABLETYPE;//HTTT(1.Thanh toán trực tuyến, 2.Tiền mặt, 3.Cà thẻ, 4.Chuyển khoản)
    public int TRANSACTIONTYPEID;//Cổng thanh toán trực tuyến(Lưu khi PAYMENTTYPE=1)
    public long PAYTIME;//Thời gian thanh toán
    public float TOTALAMOUNT;//Tổng tiền thanh toán
    @Override
    public String toString() {

        return "[{\"Id\": \""+ID+"\"," +
                "\"CUSTOMERID\": \""+CUSTOMERID+"\"," +
                "\"BRANDID\": \""+BRANDID+"\"," +
                "\"SALEORDERID\": \""+SALEORDERID+"\"," +
                "\"PAYABLETYPE\": \""+PAYABLETYPE+"\"," +
                "\"TRANSACTIONTYPEID\": \""+TRANSACTIONTYPEID+"\"," +
                "\"PAYTIME\": \""+PAYTIME+"\"," +
                "\"TOTALAMOUNT\": \""+TOTALAMOUNT+"\"" +
                "}]";
    }
}
