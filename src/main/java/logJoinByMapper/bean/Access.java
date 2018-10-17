package logJoinByMapper.bean;

import common.DateUtil;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class Access implements WritableComparable<Access> {
    private String ip;
    private Date accessTime;
    private String url;

    public Access() {
    }

    public String getIp() {
        return ip;
    }

    public Date getAccessTime() {
        return accessTime;
    }

    public String getUrl() {
        return url;
    }

    public void set(String[] fields) {
        ip = fields[0];
        accessTime = DateUtil.parse(fields[1]);
        url = fields[2];
    }

    @Override
    public int compareTo(Access o) {
        return url.compareTo(o.url);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(ip + "\n");
        out.writeBytes(url + "\n");
        out.writeLong(accessTime.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ip = in.readLine();
        url = in.readLine();
        accessTime = new Date(in.readLong());
    }

    @Override
    public String toString() {
        return url;
    }
}
