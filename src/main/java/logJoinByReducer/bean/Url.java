package logJoinByReducer.bean
        ;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Url implements WritableComparable<Url> {
    private String url;
    private int sum;

    public void set(String url, String sumStr) {
        this.url = url;
        sum = Integer.valueOf(sumStr);
    }

    @Override
    public int compareTo(Url o) {
        return Integer.compare(o.sum, sum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(url);
        out.writeInt(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        url = in.readUTF();
        sum = in.readInt();
    }

    @Override
    public String toString() {
        return url + "," + sum;
    }
}
