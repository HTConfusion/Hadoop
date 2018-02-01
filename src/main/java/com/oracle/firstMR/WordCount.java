package com.oracle.firstMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordCount {

    public static class ForMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(" ");
            for (String s : str) {
                okey.set(s);
                ovalue.set(1);
                context.write(okey, ovalue);
            }
        }
    }

    public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for(IntWritable i:values){
                num += 1;
            }
            okey.set(key);
            ovalue.set(num);
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Path path = new Path("E:\\wordCountResult");

        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(new URI("file:///E://wordCountResult"),new Configuration());
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileInputFormat.setInputPaths(job,new Path("E:\\wordCount.txt"));
        FileOutputFormat.setOutputPath(job,path);
        job.waitForCompletion(true);
    }
}
