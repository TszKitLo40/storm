package storm.starter.poc;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by robert on 25/5/16.
 */
public class Record implements Serializable {
    public long orderNo;
    public String accountId;
    public double price;
    public int volume;
    public int secCode;
    public String time;

    public Record(long orderNo, String accountId, double price, int volume, int secCode, String time) {
        this.orderNo = orderNo;
        this.accountId = accountId;
        this.price = price;
        this.volume = volume;
        this.secCode = secCode;
        this.time = time;
    }

    public static Comparator<Record> getComparator() {
        return new Comparator<Record>() {
            @Override
            public int compare(Record record, Record record2) {
                return Double.compare(record.price, record2.price);
            }
        };
    }

    public static Comparator<Record> getReverseComparator() {
        return new Comparator<Record>() {
            @Override
            public int compare(Record record, Record record2) {
                return Double.compare(record2.price, record.price);
            }
        };
    }
}
