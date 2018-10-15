package log;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Queue;

import static java.util.Map.Entry;

public class Top10 {

    public static class Top10Mapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, ArrayList<String>> map = new HashMap<>();

        @Override
        protected void map(LongWritable offset, Text line, Context context) {
            String[] fields = StringUtils.split(line.toString(), '\t');
            if (!map.containsKey(fields[0])) {
                ArrayList<String> list = new ArrayList<>() {
                    {
                        add(fields[1]);
                    }
                };
                map.put(fields[0], list);
            } else {
                if (!map.get(fields[0]).contains(fields[1])) {
                    map.get(fields[0]).add(fields[1]);
                }
            }
        }


        Text url = new Text();
        Text user = new Text();
        @Override
        protected void cleanup(Context context) {
            for (Entry<String, ArrayList<String>> e : map.entrySet()) {
                for (String s : e.getValue()) {
                    url.set(e.getKey());
                    user.set(s);
                    try {
                        context.write(url, user);
                    } catch (IOException | InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
    }

    public static class Top10Reducer extends Reducer<Text, Text, Text, LongWritable> {

        Queue<Url> urlQueue = new PriorityQueue<>();
        @Override
        protected void reduce(Text url, Iterable<Text> users, Context context) {
            int count = 0;
            while (users.iterator().hasNext()) {
                count++;
                users.iterator().next();
            }

            urlQueue.add(new Url(url.toString(), count));
        }

        Text outKey = new Text();
        LongWritable outValue = new LongWritable();
        @Override
        protected void cleanup(Context context) {
            int k = 10;
            for (int i = 0; i < k; i++) {
                Url url = urlQueue.poll();
                if (url == null) {
                   break;
                }
                outKey.set(url.getUrl());
                outValue.set(url.getCount());
                try {
                    context.write(outKey, outValue);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.10:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(Top10.class);
        job.setMapperClass(Top10Mapper.class);
        job.setReducerClass(Top10Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        String inputPath = "e:/tmp/log/input_top10";
        String inputPath = "/log/input_top10/";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
//        String outputPath = "e:/tmp/log/output_top10";
        String outputPath = "/log/output_top10/";
        Path outputDir = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }

}
