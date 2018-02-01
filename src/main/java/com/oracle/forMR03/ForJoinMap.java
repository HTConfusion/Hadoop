package com.oracle.forMR03;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by LHT on 2018/1/19
 */
public class ForJoinMap {

    public static class ForMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Map<String,String> cacheMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            /**
             * 读取缓存文件数组
             */
            //获取第一个文件
            URI cacheURI = context.getCacheFiles()[0];
            //设置缓冲字符流
            BufferedReader br = new BufferedReader(new FileReader(new File(cacheURI)));
            Counter cacheLine = context.getCounter("MapJoin", "cacheLine");
            String temp;
            while ((temp = br.readLine()) != null) {
                System.err.println(" [CacheFile] : " + temp);
                cacheLine.increment(1L);
                //存入map
                String[] str = temp.split("\t");
                cacheMap.put(str[0],str[1]);
            }
        }
        private Text okey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split("\t");
            //查找主键对应值
            String pk = cacheMap.get(strs[2]);
            okey.set(line+"\t"+pk);
            context.write(okey,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        Job job = Job.getInstance();
        job.setCacheFiles(new URI[10]);
        job.addCacheFile(new URI("file:///E:/File/BigData/TestData/20180119/jionData/map/phoneinfo.txt"));
        JobUtil.commitJob(ForJoinMap.class, "E:\\File\\BigData\\TestData\\20180119\\jionData\\map\\userinfo.txt", "", new URI("file:///E:/File/BigData/TestData/20180119/jionData/map/phoneinfo.txt"));
    }
}
