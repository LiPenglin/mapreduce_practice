package log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class LoginJoinVisitOnIp {

    private static Configuration conf;

    public static class JoinMapper extends Mapper<LongWritable, Text, Access, User> {
        ArrayList<User> users = new ArrayList<>();
        URI[] cacheFiles;
        FileSystem fs;
        InputStreamReader in;
//        FileReader in;
        BufferedReader reader;
        String line;

        @Override
        protected void setup(Context context) {
            HashMap<String, User> loginUsers = new HashMap<>();
            try {
                cacheFiles = context.getCacheFiles();

                fs = FileSystem.get(conf);
                in = new InputStreamReader(fs.open(new Path(cacheFiles[0])), StandardCharsets.UTF_8);
                reader = new BufferedReader(in);
//                in = new FileReader("e:/tmp/log/input/login.log");
//                reader = new BufferedReader(in);

                while ((line = reader.readLine()) != null) {
                    //tom,192.168.1.11,2017-11-20 10:00,1
                    String[] fields = line.split(",");
                    if (Integer.parseInt(fields[3]) == 1) {
                        User user = new User(fields);
                        loginUsers.put(fields[0], user);
                    } else {
                        User user = loginUsers.get(fields[0]);
                        user.setOffline(fields[2]);
                        users.add(user);
                        loginUsers.remove(fields[0]);
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Access access = new Access();
        @Override
        protected void map(LongWritable offset, Text line, Context context) {
            String[] fields = StringUtils.split(line.toString(), ',');
            access.set(fields);
            for (User user : users) {
                if (user.equals2Access(access)) {
                    try {
                        context.write(access, user);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static class JoinReduce extends Reducer<Access, User, Access, User> {
        @Override
        protected void reduce(Access access, Iterable<User> users, Context context) {
            for (User user : users) {
                try {
                    context.write(access, user);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0");

        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.10:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(LoginJoinVisitOnIp.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);

        job.setOutputKeyClass(Access.class);
        job.setOutputValueClass(User.class);

        job.addCacheFile(new URI("hdfs://192.168.1.10:8020/log/cache/login.log"));
//        String inputPath = "e:/tmp/log/input";
        String inputPath = "/log/input/";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
//        String outputPath = "e:/tmp/log/output";
        String outputPath = "/log/output/";
        Path outputDir = new Path(outputPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }

}
