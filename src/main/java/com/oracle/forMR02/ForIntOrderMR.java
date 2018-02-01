package com.oracle.forMR02;

import com.oracle.util.MyJobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LHT on 2018/1/18
 */
public class ForIntOrderMR {

    public static class ForMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

        private IntWritable okey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int i = Integer.parseInt(line);
            System.err.println("map="+i);
            okey.set(i);
            context.write(okey, NullWritable.get());
        }
    }

    public static class ForReducer extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable>{

        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.err.println("reduce="+key);
            context.write(key,NullWritable.get());
        }
    }

    public static class MyComparable extends IntWritable.Comparator{

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException {
        Job job = Job.getInstance();
        job.setSortComparatorClass(MyComparable.class);
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        MyJobUtil.jobPathClear(job,"E:\\File\\BigData\\TestData\\20180118\\Int.txt","IntOrder");
    }
}