package logJoinByMapper.components;

import logJoinByMapper.bean.Access;
import logJoinByMapper.bean.User;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class LoginJoinVisitOnIp {

    public static class JoinMapper extends Mapper<LongWritable, Text, Access, User> {
        ArrayList<User> users = new ArrayList<>();
//        URI[] cacheFiles;
//        FileSystem fs;
//        InputStreamReader in;
        FileReader in;
        BufferedReader reader;
        String line;

        @Override
        protected void setup(Context context) {
            HashMap<String, User> loginUsers = new HashMap<>();
            try {
//                cacheFiles = context.getCacheFiles();

//                fs = FileSystem.get(conf);
//                in = new InputStreamReader(fs.open(new Path(cacheFiles[0])), StandardCharsets.UTF_8);
//                reader = new BufferedReader(in);
                in = new FileReader("e:/tmp/log/cache/login.log");
                reader = new BufferedReader(in);

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

}
