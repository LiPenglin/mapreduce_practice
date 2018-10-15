package log;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class User implements Writable {
    private String username;
    private String ip;
    private Date online;
    private Date offline;

    public User() {
    }

    public User(String[] fields) {
        username = fields[0];
        ip = fields[1];
        online = DateUtil.parse(fields[2]);

    }

    public void setOffline(String offline) {
        this.offline = DateUtil.parse(offline);
    }

    public boolean equals2Access(Access a) {
        return ip.equals(a.getIp())
                && (a.getAccessTime().after(online) && a.getAccessTime().before(offline));
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(username + "\n");
        out.writeBytes(ip + "\n");
        out.writeLong(online.getTime());
        out.writeLong(offline.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        username = in.readLine();
        ip = in.readLine();
        online = new Date(in.readLong());
        offline = new Date(in.readLong());
    }

    @Override
    public String toString() {
        return username + ","
                + ip + ","
                + DateUtil.format(online) + ","
                + DateUtil.format(offline);
    }
}
