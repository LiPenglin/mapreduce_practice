package topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class Topk {

    //mapper
    public static class TopkMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {

        Text outKey = new Text();
        LongWritable outValue = new LongWritable();
        String[] line;

        public void map(LongWritable key, Text value, Context context) {
            line = value.toString().split("\t");
            outKey.set(line[0]);
            outValue.set(Long.parseLong(line[1]));

            try {
                context.write(outKey, outValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //reducer
    public static class TopkReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) {
            StreamSupport.stream(values.spliterator(), false)
                    .map(LongWritable::get)
                    .sorted(Comparator.reverseOrder())
                    .limit(3)
                    .map(LongWritable::new)
                    .forEach(writeToContext(key, context));
        }

        Consumer<LongWritable> writeToContext(Text key, Context context) {
            return value -> {
                try {
                    context.write(key, value);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            };
        }
    }

    //main
    public static void main(String []args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.1.10:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(Topk.class);
        job.setMapperClass(TopkMapper.class);
        job.setReducerClass(TopkReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/topk/input"));
        FileOutputFormat.setOutputPath(job, new Path("/topk/output"));

        System.exit(job.waitForCompletion(true)? 0: 1);
    }


}
