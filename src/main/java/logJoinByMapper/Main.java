package logJoinByMapper;

import logJoinByMapper.bean.Access;
import logJoinByMapper.bean.User;
import logJoinByMapper.components.LoginJoinVisitOnIp;
import logJoinByMapper.components.Top10;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {

    static String inputPath;
    static String outputPath;
    static Path outputDir;

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://192.168.1.10:8020");

        FileSystem fs = FileSystem.get(conf);

        Job joinJob = Job.getInstance(conf);
        joinJob.setJarByClass(LoginJoinVisitOnIp.class);
        joinJob.setMapperClass(LoginJoinVisitOnIp.JoinMapper.class);
        joinJob.setReducerClass(LoginJoinVisitOnIp.JoinReduce.class);
        joinJob.setOutputKeyClass(Access.class);
        joinJob.setOutputValueClass(User.class);
//        joinJob.addCacheFile(new URI("hdfs://192.168.1.10:8020/log/cache/login.log"));
        inputPath = "e:/tmp/log/input";
        FileInputFormat.setInputPaths(joinJob, new Path(inputPath));
        outputPath = "e:/tmp/log/top10_input";
        outputDir = new Path(outputPath);
        deleteOutputDir(fs, outputDir);
        FileOutputFormat.setOutputPath(joinJob, outputDir);

        Job top10Job = Job.getInstance(conf);
        top10Job.setJarByClass(Top10.class);
        top10Job.setMapperClass(Top10.Top10Mapper.class);
        top10Job.setReducerClass(Top10.Top10Reducer.class);
        top10Job.setOutputKeyClass(Text.class);
        top10Job.setOutputValueClass(Text.class);
        inputPath = "e:/tmp/log/top10_input";
        FileInputFormat.setInputPaths(top10Job, new Path(inputPath));
        outputPath = "e:/tmp/log/top10_output";
        outputDir = new Path(outputPath);
        deleteOutputDir(fs, outputDir);
        FileOutputFormat.setOutputPath(top10Job, outputDir);

        ControlledJob joinCJob = new ControlledJob(joinJob.getConfiguration());
        ControlledJob top10CJob = new ControlledJob(top10Job.getConfiguration());

        joinCJob.setJob(joinJob);
        top10CJob.setJob(top10Job);

        top10CJob.addDependingJob(joinCJob);

        JobControl jobControl = new JobControl("logJoinByMapper url access sum top10");

        jobControl.addJob(joinCJob);
        jobControl.addJob(top10CJob);

        new Thread(jobControl).start();

        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
        }
        jobControl.stop();
    }

    private static void deleteOutputDir(FileSystem fs, Path outputDir) throws IOException {
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
    }
}
