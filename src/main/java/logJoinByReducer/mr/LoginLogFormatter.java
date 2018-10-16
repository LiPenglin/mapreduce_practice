package logJoinByReducer.mr;

import common.DateUtil;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

public class LoginLogFormatter {

    public static class LogFormatMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text outputKey = new Text();
        Text outputValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] fields = StringUtils.split(value.toString(), ',');
            outputKey.set(fields[0] + "," + fields[1]);
            outputValue.set(fields[2] + "," + fields[3]);
            try {
                context.write(outputKey, outputValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LogFormatReducer extends Reducer<Text, Text, Text, NullWritable> {

        ArrayList<Date> loginRecorder = new ArrayList<>();
        ArrayList<Date> logoutRecorder = new ArrayList<>();
        Text outputKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            for (Text value : values) {
                String[] fields = value.toString().split(",");
                if (Integer.valueOf(fields[1]) == 1) {
                    loginRecorder.add(DateUtil.parse(fields[0]));
                } else {
                    logoutRecorder.add(DateUtil.parse(fields[0]));
                }
            }
            Collections.sort(loginRecorder);
            Collections.sort(logoutRecorder);

            for (int i = 0; i < loginRecorder.size(); i++) {
                outputKey.set(key + "," +
                        DateUtil.format(loginRecorder.get(i)) + "," +
                        DateUtil.format(logoutRecorder.get(i)));
                try {
                    context.write(outputKey, NullWritable.get());
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

            loginRecorder.clear();
            logoutRecorder.clear();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LoginLogFormatter.class);
        job.setMapperClass(LogFormatMapper.class);
        job.setReducerClass(LogFormatReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String inputPath = "e:/tmp/log/login_log_formatter_input";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        String outputPath = "e:/tmp/log/login_log_formatter_output";
        Path outputDir = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }

}
