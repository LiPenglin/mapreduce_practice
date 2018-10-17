package logJoinByReducer.main;

import logJoinByReducer.bean.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import java.net.URISyntaxException;
import java.util.HashSet;

public class UrlAccessCounter {

    public static class CounterMapper extends Mapper<LongWritable, Text, Text, User> {

        Text outputKey = new Text();
        User outputValue = new User();
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] fields = StringUtils.split(value.toString(), '|');
            outputKey.set(fields[3]);
            outputValue.set(fields[4], fields[5], fields[6]);
            try {
                context.write(outputKey, outputValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class CounterReducer extends Reducer<Text, User, Text, LongWritable> {
        HashSet<User> counter = new HashSet<>();
        LongWritable outputValue = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<User> values, Context context) {
            for (User value : values) {
                counter.add(value);
            }
           outputValue.set(counter.size());
            try {
                context.write(key, outputValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            counter.clear();
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(UrlAccessCounter.class);
        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(User.class);

        String inputPath = "e:/tmp/logJoinByMapper/url_access_counter_input";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        String outputPath = "e:/tmp/logJoinByMapper/url_access_counter_output";
        Path outputDir = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
