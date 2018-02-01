package com.test.one;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce基础——应用案例——网站KPI统计
 * Created by LHT on 2018/1/24
 */
public class WebKPI {
    public static class Utile {
        public static Map<String, String> logUtil(String line) {
            Map<String, String> map = new HashMap<>();
            String[] str = line.split(" ");
            map.put("ip", str[0]);
            map.put("time", str[3].substring(1));
            if (str[6].contains("?")) {
                map.put("page", str[6].split("\\?")[0]);
            } else {
                map.put("page", str[6]);
            }
            String web = "";
            if (str[10].contains("?")) {
                web = str[10].split("\\?")[0].substring(1);
            } else {
                web = str[10].substring(1, str[10].length() - 1);
            }
            if (!"".equals(web)&&!"-".equals(web)) {
                String[] webs = web.split("/");
                web = webs[0]+"//"+webs[2];
                map.put("web", web);
            }
            String browser = "";
            try {
                browser = str[11].split("/")[0].substring(1);
                if (!("".equals(browser)) && !browser.contains("-")) {
                    map.put("browser", browser);
                }
            } catch (Exception e) {
                System.out.println("---------------");
            }
            //System.err.println(map.get("ip") + "--" + map.get("time") + "--" + map.get("web") + "--" + map.get("page") + "--" + map.get("browser"));
            return map;
        }
    }

    public static class One {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

            private Text okey = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                Map<String, String> map;
                String line = value.toString();
                map = Utile.logUtil(line);
                String browser = map.get("browser");
                if (!map.containsKey("browser")) {
                    return;
                } else {
                    okey.set(browser);
                    context.write(okey, NullWritable.get());
                }
            }
        }

        public static class ForReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
            private Text okey = new Text();

            @Override
            protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                for (NullWritable n : values) {
                    count++;
                }
                okey.set(key + " - " + count);
                context.write(okey, NullWritable.get());
            }
        }
    }

    public static class Two {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
            private Text okey = new Text();
            private Text ovalue = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                Map<String, String> map;
                String line = value.toString();
                map = Utile.logUtil(line);
                String page = map.get("page");
                String ip = map.get("ip");
                okey.set(page);
                ovalue.set(ip);
                context.write(okey, ovalue);
            }
        }

        public static class ForReducer extends Reducer<Text, Text, Text, Text> {
            private Text okey = new Text();
            private Text ovalue = new Text();

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                StringBuilder sb = new StringBuilder();
                for (Text t : values) {
                    count++;
                    sb.append("\t" + t);
                }
                okey.set(key + ":");
                ovalue.set("count:" + count + sb.toString());
                context.write(okey, ovalue);
            }
        }
    }

    public static class Three {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
            private Text okey = new Text();
            private Text ovalue = new Text();
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                Map<String, String> map;
                String line = value.toString();
                map = Utile.logUtil(line);
                String ip = map.get("ip");
                if (!map.containsKey("web")) {
                    return;
                } else {
                    String web = map.get("web");
                    okey.set(web);
                    ovalue.set(ip);
                    context.write(okey,ovalue);
                }
            }
        }

        public static class ForReducer extends Reducer<Text,Text,Text,Text>{
            private Text okey = new Text();
            private Text ovalue = new Text();
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                Set<String> set = new HashSet<>();
                for(Text n:values){
                    System.err.println(n.toString());
                    set.add(n.toString());
                }
                count = set.size();
                okey.set(key.toString()+":");
                ovalue.set("count:"+count);
                context.write(okey,ovalue);
            }
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(Three.class, "E:\\File\\BigData\\Test\\MapReduce经典案例 WordCount 练习题及答案\\实验数据", "");
        //System.out.println("\\");
    }
}
