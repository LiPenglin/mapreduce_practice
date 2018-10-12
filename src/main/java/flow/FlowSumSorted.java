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

public class FlowSumSorted {

    public static class FlowSumSortedMapper extends
            Mapper<LongWritable, Text, Flow, Text> {

        Flow flow = new Flow();
        Text phoneNumber = new Text();

        @Override
        protected void map(LongWritable offset, Text text, Context context) {
            String[] fields = StringUtils.split(text.toString(), '\t');
            flow.set(Long.parseLong(fields[1]), Long.parseLong(fields[2]));
            phoneNumber.set(fields[0]);
            try {
                context.write(flow, phoneNumber);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class FlowSumSortedReducer extends
            Reducer<Flow, Text, Text, Flow> {
        @Override
        protected void reduce(Flow flow, Iterable<Text> phoneNumber, Context context) {
            try {
                context.write(phoneNumber.iterator().next(), flow);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.1.10:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumSorted.class);
        job.setMapperClass(FlowSumSortedMapper.class);
        job.setReducerClass(FlowSumSortedReducer.class);

        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/flow/output"));
        FileOutputFormat.setOutputPath(job, new Path("/flow/output_sorted"));

        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
