package flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Map.*;

public class FlowSumSortedCache {


    public static class FlowSumMapper extends
            Mapper<LongWritable, Text, Text, Flow> {

        Flow flow = new Flow();
        Text phoneNumber = new Text();
        protected void map(LongWritable offset, Text text, Context context) {
            String[] fields = StringUtils.split(text.toString(), '\t');

            phoneNumber.set(fields[1]);

            long upstreamFlow = Long.parseLong(fields[fields.length - 3]);
            long downstreamFlow = Long.parseLong(fields[fields.length - 2]);
            flow.set(upstreamFlow, downstreamFlow);

            try {
                context.write(phoneNumber, flow);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class FlowSumReducer extends
            Reducer<Text, Flow, Text, Flow> {

        TreeMap<Flow, Text> cache = new TreeMap<>();
        protected void reduce(Text phoneNumber, Iterable<Flow> FlowList, Context context) {
            Flow flow = new Flow();
            FlowList.forEach(flow::add);
            cache.put(flow, phoneNumber);
        }

        @Override
        protected void cleanup(Context context) {
            for (Entry<Flow, Text> entry : cache.entrySet()) {
                try {
                    context.write(entry.getValue(), entry.getKey()) ;
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumSortedCache.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);

        FileInputFormat.setInputPaths(job, new Path("e:/flow/input"));
        FileOutputFormat.setOutputPath(job, new Path("e:/flow/output"));

        System.exit(job.waitForCompletion(true)? 0: 1);
    }

}

