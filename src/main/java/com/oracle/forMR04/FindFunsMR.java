package com.oracle.forMR04;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 找粉丝
 * Created by LHT on 2018/1/23
 */
public class FindFunsMR {

    public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text okey = new Text();
        private Text ovalue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = StringUtils.split(value.toString(), ':');
            //将文件数据切碎，置换kye与value
            String forV = str[0];
            String[] friends = str[1].split(",");
            for (String f : friends) {
                okey.set(f);
                ovalue.set(forV);
                context.write(okey, ovalue);
            }
        }
    }

    public static class ForReducer extends Reducer<Text, Text, Text, Text> {
        private Text okey = new Text();
        private Text ovalue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text t:values){
                sb.append(t+",");
            }
            okey.set(key.toString()+":");
            ovalue.set(sb.toString());
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(FindFunsMR.class,"E:\\File\\BigData\\TestData\\20180123\\friend","");
    }
}
