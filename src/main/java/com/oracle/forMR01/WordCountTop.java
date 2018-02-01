package com.oracle.forMR01;

import com.oracle.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class WordCountTop {

    public static class ForMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private int max;
        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        //每一行执行一次
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str = line.split("\t");
            int times = Integer.parseInt(str[1]);
            if (times > max) {
                max = times;
                okey.set(str[0]);
            }
        }

        //整个mapper只执行一次
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ovalue.set(max);
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) throws Exception{

        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path path = new Path("E:/File/BigData/TestData/Result/top");
        FileSystem fs = FileSystem.get(new URI("file:///E:/File/BigData/TestData/Result/top"),new Configuration());
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileInputFormat.setInputPaths(job,new Path("E:\\File\\BigData\\TestData\\Result\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,path);
        job.waitForCompletion(true);
    }
}
