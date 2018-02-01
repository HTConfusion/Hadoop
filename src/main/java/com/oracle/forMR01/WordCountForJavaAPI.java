package com.oracle.forMR01;

import com.oracle.util.JobUtil;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by LHT on 2018/1/17.
 */
public class WordCountForJavaAPI {


    public static class ForMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        public ForMapper() {
            System.out.println("========================");
        }

        private Text okey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //正则匹配
            Pattern pattern = Pattern.compile("\\w+");//[a-zA-Z_0-9] 需转译
            Matcher matcher = pattern.matcher(line);
            while(matcher.find()){
                String word = matcher.group();
                okey.set(word);
                context.write(okey,NullWritable.get());
            }
        }
    }

    public  static class ForReducer extends Reducer<Text,NullWritable,Text,IntWritable>{

        private IntWritable ovalue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int num = 0;
            for(NullWritable n:values){
                num++;
            }
            ovalue.set(num);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
//        Job job = Job.getInstance();
//        job.setMapperClass(ForMapper.class);
//        job.setReducerClass(ForReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        Path path = new Path("E:/File/BigData/TestData/Result");
//        FileSystem fs = FileSystem.get(new URI("file:///E:/File/BigData/TestData/Result"),new Configuration());
//        if(fs.exists(path)){
//            fs.delete(path,true);
//        }
//        FileInputFormat.setInputPaths(job,new Path("E:\\File\\BigData\\TestData\\20180117\\wordCount"));
//        FileOutputFormat.setOutputPath(job,path);
//        job.waitForCompletion(true);

        JobUtil.commitJob(WordCountForJavaAPI.class,"E:\\File\\BigData\\TestData\\20180117\\wordCount","");
    }
}
