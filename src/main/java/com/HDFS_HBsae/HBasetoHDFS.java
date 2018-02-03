package com.HDFS_HBsae;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 从hbase取，存入hdfs
 * Created by LHT on 2018/2/3
 */
public class HBasetoHDFS {
    public static class ForMapper extends TableMapper<Text,Text>{
        private Text okey = new Text();
        private Text ovalue = new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //key 为 RowKey
            String row = "";
            String family = "";
            String qualifier = "";
            String values = "";
            for(Cell c:value.listCells()){
                row = new String(CellUtil.cloneRow(c));
                family = new String(CellUtil.cloneFamily(c));
                qualifier = new String(CellUtil.cloneQualifier(c));
                values = new String(CellUtil.cloneValue(c));
            }
            String temp = row+"\t"+family+"\t"+qualifier+"\t"+values;
            System.err.println("key="+key.toString());
            System.err.println("value="+temp);
            okey.set(key.toString());
            ovalue.set(temp);
            context.write(okey,ovalue);
        }
    }

    public static class ForReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t:values){
                context.write(key,t);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //获取zookeeper资源
        config.set("hbase.zookeeper.quorum", "master,slave1");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        //置入config
        Job job = Job.getInstance(config);
        //设定jar包主类
        job.setJarByClass(HBasetoHDFS.class);
        //常规设置
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设定scan--查找范围，过滤等
        Scan scan = new Scan(Bytes.toBytes("row-1"), Bytes.toBytes("row-2"));
        scan.setMaxVersions();
        //设定输入
        TableMapReduceUtil.initTableMapperJob("tablename",scan,ForMapper.class,Text.class,Text.class,job,false);
        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://master:8020/hbaseTohdfs/"));
        job.waitForCompletion(true);
    }
}
