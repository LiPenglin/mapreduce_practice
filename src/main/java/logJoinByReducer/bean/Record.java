package logJoinByReducer.bean;

import common.DateUtil;
import logJoinByReducer.components.State;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class Record implements Writable, Cloneable {

    private State type ;
    private String ip ;
    private Date accessTime = new Date();
    private String url = "";
    private String username = "";
    private Date onlineTime = new Date();
    private Date offlineTime = new Date();

    public State getType() {
        return type;
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

    public String getUsername() {
        return username;
    }

    public Date getOnlineTime() {
        return onlineTime;
    }

    public Date getOfflineTime() {
        return offlineTime;
    }

    public void setType(State type) {
        this.type = type;
    }

    public void setAccessTime(Date accessTime) {
        this.accessTime = accessTime;
    }

    public void setOnlineTime(Date onlineTime) {
        this.onlineTime = onlineTime;
    }

    public void setOfflineTime(Date offlineTime) {
        this.offlineTime = offlineTime;
    }

    public void setVisit(String ip, String time, String url) {
        type = State.VISIT;
        this.ip = ip;
        accessTime = DateUtil.parse(time);
        this.url = url;
    }

    public void setLogin(String username, String ip, String onlineTimeStr, String offlineTimeStr) {
        type = State.LOGIN;
        this.ip = ip;
        this.username = username;
        onlineTime = DateUtil.parse(onlineTimeStr);
        offlineTime = DateUtil.parse(offlineTimeStr);
    }

    public void set(Record login) {
        type = State.JOIN;
        username = login.getUsername();
        onlineTime = login.getOnlineTime();
        offlineTime = login.getOfflineTime();
    }

    public boolean equals2Visit(Record visit) {
        return  visit.getAccessTime().after(onlineTime)
                && visit.getAccessTime().before(offlineTime);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(type.name());
        out.writeUTF(ip);
        out.writeUTF(DateUtil.format(accessTime));
        out.writeUTF(url);
        out.writeUTF(username);
        out.writeUTF(DateUtil.format(onlineTime));
        out.writeUTF(DateUtil.format(offlineTime));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = State.valueOf(in.readUTF());
        ip = in.readUTF();
        accessTime = DateUtil.parse(in.readUTF());
        url = in.readUTF();
        username = in.readUTF();
        onlineTime = DateUtil.parse(in.readUTF());
        offlineTime = DateUtil.parse(in.readUTF());
    }

    @Override
    protected Object clone() {
        try {
            Record clone = (Record) super.clone();
            clone.setAccessTime((Date) accessTime.clone());
            clone.setOnlineTime((Date) onlineTime.clone());
            clone.setOfflineTime((Date) offlineTime.clone());
            return clone;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public String toString() {
        String verticalBar = "|";
        return type + verticalBar +
                ip + verticalBar +
                DateUtil.format(accessTime) + verticalBar +
                url + verticalBar +
                username + verticalBar +
                DateUtil.format(onlineTime) + verticalBar +
                DateUtil.format(offlineTime) + verticalBar;
    }
}
