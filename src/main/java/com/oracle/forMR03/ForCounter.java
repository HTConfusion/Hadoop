package com.oracle.forMR03;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LHT on 2018/1/19
 */
public class ForCounter {

    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //设置字符指定 组 & 名
            Counter senWords = context.getCounter("oracle","senWord");
            String line = value.toString();
            if(line.contains("the")){
                senWords.increment(1L);
            }
            //设置枚举
            Counter lineNum = context.getCounter(LineCounter.NUM);
            Counter errorNum = context.getCounter(LineCounter.ERROR);
            lineNum.increment(1L);
            try{
                Integer.parseInt(line);
            }catch (Exception e){
                errorNum.increment(1L);
            }
        }
    }

    public static enum LineCounter{//枚举
        NUM,
        ERROR
    }

    /*public static class ForReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }*/

    public static void main(String[] args) {
        JobUtil.commitJob(ForCounter.class,"E:\\File\\BigData\\TestData\\20180119\\counter","");
    }
}
