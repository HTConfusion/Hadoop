package com.test.one;

import com.oracle.util.JobUtil;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * MapReduce基础——网站微博数据统计分析
 * Created by LHT on 2018/1/24
 */
public class WeiBoData {
    public static class Util {
        public Map<String, String> xmlUtil(String line) {
            String[] str = line.trim().split("=");
            if (str.length == 7) {
                Map<String, String> map = new HashMap<>();
                String id = str[1].substring(1, str[1].length() - 8);
                String postId = str[2].substring(1, str[2].length() - 7);
                String score = str[3].substring(1, str[3].length() - 6);
                String text = str[4].substring(1, str[4].length() - 14);
                String creationDate = str[5].substring(1, str[5].length() - 8);
                String userId = str[6].substring(1, str[6].length() - 4);
                System.out.println(id + "--" + postId + "--" + score + "--" + text + "--" + creationDate + "--" + userId);
                map.put("Id", id);
                map.put("PostId", postId);
                map.put("Score", score);
                map.put("Text", text);
                map.put("CreationDate", creationDate);
                map.put("UserId", userId);
                return map;
            } else if (str.length == 6 && line.contains("UserId")) {
                Map<String, String> map = new HashMap<>();
                String id = str[1].substring(1, str[1].length() - 8);
                String postId = str[2].substring(1, str[2].length() - 7);
                //String score = str[3].substring(1, str[3].length() - 6);
                String text = str[3].substring(1, str[3].length() - 6);
                String creationDate = str[4].substring(1, str[4].length() - 8);
                String userId = str[5].substring(1, str[5].length() - 4);
                System.out.println(id + "--" + postId + "--" + text + "--" + creationDate + "--" + userId);
                map.put("Id", id);
                map.put("PostId", postId);
                //map.put("Score", score);
                map.put("Text", text);
                map.put("CreationDate", creationDate);
                map.put("UserId", userId);
                return map;
            }
            return null;
        }
    }

    public static class One {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
            private Util util = new Util();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                Counter counter = context.getCounter(WeiBo.piceCount);
                if (util.xmlUtil(line) != null) {
                    counter.increment(1L);//2202
                }
            }
        }

        public static enum WeiBo {
            piceCount
        }
    }

    public static class Two {
        public static class ForMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
            private Util util = new Util();
            private Map<String, String> map = new HashMap<>();
            private IntWritable okey = new IntWritable();
            private Text ovalue = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                map = util.xmlUtil(line);
                if (map != null) {
                    okey.set(Integer.parseInt(map.get("UserId")));
                    ovalue.set(map.get("CreationDate") + "\t" + map.get("Id"));
                    context.write(okey, ovalue);
                }
            }
        }

        public static class ForReducer extends Reducer<IntWritable, Text, Text, Text> {
            private Text okey = new Text();
            private Text ovalue = new Text();

            @Override
            protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String min = "";
                String max = "";
                int mx = 0;
                int mi = 0;
                int count = 0;
                for (Text t : values) {
                    String[] str = t.toString().split("\t");
                    int id = Integer.parseInt(str[1]);
                    if (mi == 0 || id < mi) {
                        min = str[0];
                        mi = id;
                    }
                    if (id > mx) {
                        max = str[0];
                        mx = id;
                    }
                    count++;
                }
                okey.set(key + ":");
                ovalue.set("count: " + count + "\tfirst: " + min + "\tlast: " + max);
                context.write(okey, ovalue);
            }
        }

    }

    public static class Three {
        public static class ForMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
            private Util util = new Util();
            private Map<String, String> map = new HashMap<>();
            private IntWritable okey = new IntWritable();
            private IntWritable ovalue = new IntWritable();
            private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                map = util.xmlUtil(line);
                if (map != null) {
                    String dateStr = map.get("CreationDate");
                    try {
                        Date date = frmt.parse(dateStr);
                        System.err.println(date.getHours());
                        okey.set(date.getHours());
                        ovalue.set(map.get("Text").length());
                        context.write(okey, ovalue);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public static class ForReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
            private Text okey = new Text();
            private Text ovalue = new Text();

            @Override
            protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                float count = 0;
                float sum = 0;
                for (IntWritable i : values) {
                    count++;
                    sum += i.get();
                }
                sum = sum / count;
                okey.set("Hour: " + key + " ");
                ovalue.set("count: " + count + "\tavgLength: " + sum);
                context.write(okey, ovalue);
            }
        }

    }

    public static class Ans {
        public static class SOWordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

            private Util util = new Util();
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

                // Parse the input string into a nice map
                /**
                 * 数据清洗过程开始 是一个解析数据时出于友好提示的打印帮助信息的方法。 主要获取一行xml格式的记录（有着透明的格式）并根据属性把值放入map中
                 */
                Map<String, String> parsed = new HashMap<>();
                parsed = util.xmlUtil(value.toString());

                // Grab the "Text" field, since that is what we are counting over
                String txt = parsed.get("Text");

                // .get will return null if the key is not there
                if (txt == null) {
                    // skip this record
                    return;
                }

                // Unescape the HTML because the SO data is escaped.
                txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

                // Remove some annoying punctuation
                txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
                txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a space
                // 正则表达式
                /**
                 * 数据清洗过程结束，开始计算
                 */
                // Tokenize the string, then send the tokens away
                StringTokenizer itr = new StringTokenizer(txt);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
            }
        }

        public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }

                result.set(sum);
                context.write(key, result);

            }
        }
    }


    public static void main(String[] args) {
        JobUtil.commitJob(Ans.class, "E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据", "");
    }
}
