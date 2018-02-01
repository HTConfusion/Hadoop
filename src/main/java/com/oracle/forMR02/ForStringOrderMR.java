package com.oracle.forMR02;

import com.oracle.util.MyJobUtil;
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
public class ForStringOrderMR {

    public static class ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

        private Text okey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            okey.set(line);
            System.err.println("map="+line);
            context.write(okey,NullWritable.get());
        }
    }

    public static class ForReducer extends Reducer<Text,NullWritable,Text,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.err.println("reduce="+key);
            context.write(key,NullWritable.get());
        }
    }

    public static class MyComparable extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String str1 = new String(b1,s1+1,l1-1);
            String str2 = new String(b2,s2+1,l2-1);
            int len = str1.length()-str2.length();
            if(len==0){
                len = Integer.parseInt(str1)-Integer.parseInt(str2);
            }
            return len;
        }
    }

    public static void main(String[] args) throws IOException {
        Job job = Job.getInstance();
        job.setSortComparatorClass(MyComparable.class);
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        MyJobUtil.jobPathClear(job,"E:\\File\\BigData\\TestData\\20180118\\Int.txt","StringOrder");
    }
}
