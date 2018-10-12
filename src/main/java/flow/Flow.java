package flow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements
        WritableComparable<Flow> {
    private long upstreamFlow;
    private long downstreamFlow;
    private long flowSum;

    public void set() {
        this.upstreamFlow = 0;
        this.downstreamFlow = 0;
        this.flowSum = 0;
    }

    public void set(
            long upstreamFlow,
            long downstreamFlow) {
        this.upstreamFlow = upstreamFlow;
        this.downstreamFlow = downstreamFlow;
        this.flowSum = upstreamFlow + downstreamFlow;
    }

    public void add(Flow flow) {
        this.upstreamFlow += flow.upstreamFlow;
        this.downstreamFlow += flow.downstreamFlow;
        this.flowSum += flow.flowSum;
    }

    @Override
    public int compareTo(Flow o) {
        return Long.compare(o.flowSum, flowSum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upstreamFlow);
        out.writeLong(downstreamFlow);
        out.writeLong(flowSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upstreamFlow = in.readLong();
        downstreamFlow = in.readLong();
        flowSum = in.readLong();
    }

    @Override
    public String toString() {
        return upstreamFlow + "\t" + downstreamFlow + "\t" + flowSum;
    }
}
