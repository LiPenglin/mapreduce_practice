package logJoinByMapper.components;

import logJoinByMapper.bean.Url;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
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

}
