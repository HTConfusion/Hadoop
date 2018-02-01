package com.oracle.firstMR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 第一次 map
 * @author LHT
 */
public class FirstMap {

    public static class ForMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Map-key=" + key);
            String line = value.toString();
            String[] str = line.split("\t");//按tab切分
            if (str.length == 3) {
                okey.set(str[0]);
                ovalue.set(Integer.parseInt(str[2]));
                context.write(okey, ovalue);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //Job工具
        Job job = Job.getInstance();

        job.setMapperClass(ForMapper.class);//指定Map类
        job.setOutputKeyClass(Text.class);//设置整体输出key类型
        job.setOutputValueClass(IntWritable.class);//设置整体输出value类型
        job.setMapOutputKeyClass(Text.class);//设置map输出key值类型
        job.setMapOutputValueClass(IntWritable.class);//设置map输出值类型

        FileInputFormat.setInputPaths(job,new Path("E:\\classData.txt"));//输入参数
        FileOutputFormat.setOutputPath(job,new Path("E:\\classDataResult"));//输出生成文件夹
        job.waitForCompletion(true);//提交任务



    }
}
