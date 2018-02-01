package com.oracle.forMR04;

import com.oracle.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 小文件处理1-自动解压
 * Created by LHT on 2018/1/27
 */
public class TarGZ {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String time = context.getConfiguration().get("time");
            System.err.println(time);
            //System.out.println(value.toString());
        }
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.set("time","2018-01-28 17:18:36");
        JobUtil.commitJob(TarGZ.class,"E:\\File\\BigData\\TestData\\20180122\\computer.zip","",config);
    }
}
