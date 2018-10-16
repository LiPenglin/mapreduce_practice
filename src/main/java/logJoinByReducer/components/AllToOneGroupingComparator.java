package logJoinByReducer.components;

import logJoinByReducer.bean.Url;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AllToOneGroupingComparator extends WritableComparator {
    public AllToOneGroupingComparator() {
        super(Url.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }

}
