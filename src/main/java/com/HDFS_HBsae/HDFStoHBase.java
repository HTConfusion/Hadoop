package com.HDFS_HBsae;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 从hdfs取，存入hbase
 * Created by LHT on 2018/2/1
 */
public class HDFStoHBase {
    public static class ForMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }

    }
    public static class ForReducer extends TableReducer<LongWritable, Text, LongWritable> {
        private Put put = new Put(Bytes.toBytes("row-3"));
        private int i = 0;

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                put.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col-" + i), Bytes.toBytes(t.toString()));
                context.write(key, put);
                i++;
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("hbase.zookeeper.quorum", "master,slave1");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        //置入config
        Job job = Job.getInstance(config);
        job.setJarByClass(HDFStoHBase.class);
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设定输出
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Put.class);
        //设定hdfs输入
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.44.129:8020/forCL/xwj"));
        //设定hbase输出
        TableMapReduceUtil.initTableReducerJob("tablename", ForReducer.class, job);

        job.waitForCompletion(true);

    }
}
