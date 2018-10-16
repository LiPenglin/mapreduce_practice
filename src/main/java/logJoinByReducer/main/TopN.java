package logJoinByReducer.main;

import logJoinByReducer.bean.Url;
import logJoinByReducer.components.AllToOneGroupingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;

public class TopN {
    public static class TopNMapper extends Mapper<LongWritable, Text, Url, NullWritable> {

        Url outputKey = new Url();
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] fields = StringUtils.split(value.toString(), '\t');
            outputKey.set(fields[0], fields[1]);
            try {
                context.write(outputKey, NullWritable.get());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class TopNReducer extends Reducer<Url, NullWritable, Url, NullWritable> {

        int n;
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = Integer.valueOf(conf.get("N"));
        }

        @Override
        protected void reduce(Url key, Iterable<NullWritable> values, Context context) {
            int i = 0;
            for (NullWritable ignored : values) {
                if (i >= n) {
                   return;
                }
                try {
                    context.write(key, NullWritable.get());
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();
        conf.set("N", "2");

        Job job = Job.getInstance(conf);

        job.setJarByClass(TopN.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setOutputKeyClass(Url.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(AllToOneGroupingComparator.class);

        job.setNumReduceTasks(1); // overall

        String inputPath = "e:/tmp/log/top_n_input";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        String outputPath = "e:/tmp/log/top_n_output";
        Path outputDir = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
