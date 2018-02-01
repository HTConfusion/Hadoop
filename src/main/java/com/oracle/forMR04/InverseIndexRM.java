package com.oracle.forMR04;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 倒排索引
 * 这个字在哪篇文章里，有多少
 * Created by LHT on 2018/1/23
 *
 */
public class InverseIndexRM {

    public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text okey = new Text();
        private Text ovalue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            //hadoop自带String工具
            String[] str = StringUtils.split(value.toString(), ' ');
            for (String s : str) {
                //将单词与文件名组合做键
                okey.set(s + "\t" + fileName);
                context.write(okey, ovalue);
            }
        }
    }

    public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
        private Text okey = new Text();
        private Text ovalue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            //计数：单词+文件名=键 出现次数
            for (Text t : values) {
                count++;
            }
            String[] str = key.toString().split("\t");
            //重置键与值：将单词作为键，文件名+次数作为值
            String forKey = str[0];
            String forValue = str[1] + " ---> " + count;
            okey.set(forKey);
            ovalue.set(forValue);
            context.write(okey, ovalue);
        }
    }

    public static class ForReducer extends Reducer<Text, Text, Text, Text> {
        private Text ovalue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //拼接值作为新值
            StringBuilder sb = new StringBuilder();
            for (Text t : values) {
                sb.append(" "+t);
            }
            ovalue.set(sb.toString());
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(InverseIndexRM.class, "E:\\File\\BigData\\TestData\\20180123\\inverseIndex\\data", "",new MyCombiner());
    }
}
