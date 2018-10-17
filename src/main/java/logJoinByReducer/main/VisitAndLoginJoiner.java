package logJoinByReducer.main;

import logJoinByReducer.components.State;
import logJoinByReducer.bean.Record;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class VisitAndLoginJoiner {

    public static class JoinerMapper extends Mapper<LongWritable, Text, Text, Record> {

        static final String VISIT = "visit";
        int type; // 0-visit, 1-login
        Text outputKey = new Text();
        Record outputValue;

        @Override
        protected void setup(Context context) {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            boolean startsWithVisit = inputSplit.getPath().getName().startsWith(VISIT);
            type = startsWithVisit ? 0 : 1;
            outputValue = new Record();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] fields = StringUtils.split(value.toString(), ',');
            if (type == 0) {
                outputValue.setVisit(fields[0], fields[1], fields[2]);
            } else {
                outputValue.setLogin(fields[0], fields[1], fields[2], fields[3]);
            }
            outputKey.set(outputValue.getIp());
            try {
                context.write(outputKey, outputValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class JoinerReducer extends Reducer<Text, Record, Text, NullWritable> {
        ArrayList<Record> visitList = new ArrayList<>();
        ArrayList<Record> loginList = new ArrayList<>();
        Text outputKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<Record> values, Context context) {
            for (Record value : values) {
                if (value.getType() == State.VISIT) {
                    visitList.add((Record) value.clone());
                } else {
                    loginList.add((Record) value.clone());
                }
            }

            for (Record visit : visitList) {
                for (Record login : loginList) {
                    if (login.equals2Visit(visit)) {
                        visit.set(login);
                        outputKey.set(visit.toString());
                        try {
                            context.write(outputKey, NullWritable.get());
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            visitList.clear();
            loginList.clear();
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(VisitAndLoginJoiner.class);
        job.setMapperClass(JoinerMapper.class);
        job.setReducerClass(JoinerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Record.class);

        String inputPath = "e:/tmp/logJoinByMapper/visit_and_login_joiner_input";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        String outputPath = "e:/tmp/logJoinByMapper/visit_and_login_joiner_output";
        Path outputDir = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
