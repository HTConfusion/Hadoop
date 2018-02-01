package com.test.two;


import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * MapReduce基础——电信运营商用户基站停留数据统计
 * Created by LHT on 2018/1/25
 */
public class CMCC {

    public static class Util {
        private static String[] partTimes = new String[]{"0-6", "6-12", "12-18", "18-24"};

        public static String partTimeUtil(String time) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String part = "-";
            int hour = Integer.parseInt(time.split(" ")[1].split(":")[0]);
            try {
                for (String s : partTimes) {
                    String[] str = s.split("-");
                    int start = Integer.parseInt(str[0]);
                    int end = Integer.parseInt(str[1]);
                    if (hour >= start && hour < end) {
                        part = s;
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.err.println(hour + "=======" + part);
            return part;
        }
    }

    public static class One {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
            private String fileName = "";
            private Text okey = new Text();
            private Text ovalue = new Text();

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                fileName = fileSplit.getPath().getName();
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] str = line.split("\t");
                String part = "";
                String position = "";
                if ("pos.txt".equals(fileName)) {
                    position = str[3];
                    part = Util.partTimeUtil(str[4]);
                    ovalue.set(str[4]);
                } else if ("net.txt".equals(fileName)) {
                    position = str[2];
                    part = Util.partTimeUtil(str[3]);
                    ovalue.set(str[3]);
                } else {
                    try {
                        throw new Exception("file " + fileName + " is not we need !");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                okey.set(str[0] + "\t" + part + "\t" + position);
                context.write(okey, ovalue);
            }
        }

        public static class ForReducer extends Reducer<Text, Text, Text, Text> {
            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            private Text ovalue = new Text();

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                long max = -1L;
                long min = -1L;
                for (Text e : values) {
                    try {
                        long time = sdf.parse(e.toString()).getTime() / 1000;//秒
                        if (time < min) {
                            min = time;
                        }
                        if (time > max) {
                            max = time;
                        }
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
                long minute = (max - min) % 3600 / 60;
                ovalue.set("" + minute);
                context.write(key, ovalue);
            }
        }
    }

    public static class Two {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
            private Text okey = new Text();
            private Text ovalue = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                okey.set(line.substring(0, 10));
                ovalue.set(line.substring(11));
                context.write(okey, ovalue);
            }
        }

        public static class ForReducer extends Reducer<Text, Text, Text, Text> {
            private Text ovalue = new Text();

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int max = 0;
                String load = "";
                for (Text t : values) {
                    int temp = Integer.parseInt(t.toString().split("\t")[2]);
                    if (temp > max) {
                        max = temp;
                        load = t.toString();
                    }
                }
                ovalue.set(load);
                context.write(key,ovalue );
            }
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(Two.class, "E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce基础——电信运营商用户基站停留数据统计\\数据\\一次结果数据", "");
    }
}
