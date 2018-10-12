package flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class FlowPartitioner extends Partitioner<Text, Flow> {

    private HashMap<String, Integer> provinceTable = new HashMap<>() {
        {
            put("139", 0);
            put("138", 1);
            put("137", 2);
            put("136", 3);
            put("135", 4);
        }
    };
    
    @Override
    public int getPartition(Text phoneNumber, Flow flow, int numPartitions) {
        String prefix = phoneNumber.toString().substring(0, 3);
        boolean isNotInProvinceTable = !provinceTable.keySet().contains(prefix);
        return isNotInProvinceTable ? 5 : provinceTable.get(prefix);
    }
}
