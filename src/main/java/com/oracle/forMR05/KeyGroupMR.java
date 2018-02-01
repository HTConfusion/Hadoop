package com.oracle.forMR05;

import com.oracle.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by LHT on 2018/1/27
 */
public class KeyGroupMR {
    public static class Normal {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
            private Text okey = new Text();
            private LongWritable ovalue = new LongWritable();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] line = value.toString().split(" ");
                okey.set(line[0]);
                ovalue.set(Integer.parseInt(line[1]));
                context.write(okey, ovalue);
            }
        }

        public static class ForReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
            private Text okey = new Text();

            @Override
            protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                for (LongWritable l : values) {
                    sum += l.get();
                }
                //okey.set(key.toString() + "\t" + sum);
                okey.set(key.toString().substring(0,2) + "\t" + sum);
                context.write(okey, NullWritable.get());
            }
        }
    }

    public static class MyGroup extends WritableComparator {
        //需调用构造器
        public MyGroup() {
            super(Text.class,true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return a.toString().substring(0, 2).compareTo(b.toString().substring(0, 2));
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration  config = new Configuration();
        //设置map输出可压缩
        config.setBoolean(Job.MAP_OUTPUT_COMPRESS,true);
        //设置压缩格式
        config.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class,CompressionCodec.class);
        Job job = Job.getInstance();
        job.setGroupingComparatorClass(MyGroup.class);
        //设置可以压缩
        FileOutputFormat.setCompressOutput(job,true);
        //设置压缩格式
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //或直接使用工具
        JobUtil.commitJob(Normal.class, "E:\\File\\BigData\\TestData\\20180123\\gpinput", "",new MyGroup(),new GzipCodec());
    }
}
