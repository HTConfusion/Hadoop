package com.oracle.firstMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author LHT
 */
public class FirstMR {

    /**
     * 用于整理数据
     */
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

    /**
     * 用于计算数据
     */
    public static class ForReducer extends Reducer<Text, IntWritable, Text, NullWritable> {//只输出key

        private Text okey = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //一个key执行一次
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            //结果应为 班级编号，总分
            String result = "班级：" + key.toString() + ",总分：" + sum;
            okey.set(result);
            context.write(okey, NullWritable.get());
        }
    }

    public static class ForReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {//key与value都输出

        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int num = 0;
            for (IntWritable i:values){
                num += i.get();
            }
            String result = "班级："+key.toString();
            okey.set(result);
            ovalue.set(num);
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Path path = new Path("E:\\classDataReduce");

        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);//1
        //job.setReducerClass(ForReducer2.class);//2
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);//1
        //job.setOutputValueClass(IntWritable.class);//2

        //url格式用反斜杠"/"
        //文件系统url需要相应前缀：file://  hdfs://
        FileSystem fs = FileSystem.get(new URI("file:///E://classDataReduce"), new Configuration());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job, new Path("E:\\classData.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\classDataReduce"));

        job.waitForCompletion(true);
    }
}
