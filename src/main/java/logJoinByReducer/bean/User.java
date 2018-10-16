package logJoinByReducer.bean;

import common.DateUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class User implements Writable {
    private String username;
    private Date onlineTime;
    private Date offlineTime;

    public void set(String username, String onlineTimeStr, String offlineTimeStr) {
        this.username = username;
        onlineTime = DateUtil.parse(onlineTimeStr);
        offlineTime = DateUtil.parse(offlineTimeStr);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(username);
        out.writeLong(onlineTime.getTime());
        out.writeLong(offlineTime.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        username = in.readUTF();
        onlineTime = new Date(in.readLong());
        offlineTime = new Date(in.readLong());
    }

    @Override
    public int hashCode() {
        return (username +
                DateUtil.format(onlineTime) +
                DateUtil.format(offlineTime))
                .hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        Record o = (Record) obj;
        return username.equals(o.getUrl())
                && onlineTime.equals(o.getOnlineTime())
                && offlineTime.equals(o.getOfflineTime());
    }

}
