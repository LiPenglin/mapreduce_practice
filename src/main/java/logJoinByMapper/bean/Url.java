package logJoinByMapper.bean;

public class Url implements Comparable<Url>{
    private String url;
    private int count;

    public Url(String url, int count) {
        this.url = url;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public int getCount() {
        return count;
    }

    @Override
    public int compareTo(Url o) {
        return Integer.compare(o.count, count);
    }
}
