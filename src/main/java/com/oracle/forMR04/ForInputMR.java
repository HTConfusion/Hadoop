package com.oracle.forMR04;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义map输入
 * Created by LHT on 2018/1/23
 */
public class ForInputMR {
    public static class ForMapper extends Mapper<Text,Text,Text,Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString()+"-----"+value.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //默认读取一行 \t 前为key 后为value
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\File\\BigData\\TestData\\forTestData\\testkeyValue"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\File\\BigData\\TestData\\Result\\outIn"));
        job.waitForCompletion(true);
    }
}
