package com.oracle.forMR04;

import com.oracle.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * 自定义文件分片
 * Created by LHT on 2018/1/27
 */
public class MyFIleSplit {
    /**
     * 自定义输入
     * SmailFileInputFormat为固定写法
     * CombineFileInputFormat 或 FileInputFormat
     */
    public static class SmailFileInputFormat extends CombineFileInputFormat<Text, Text> {

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;//不分片
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
            return new CombineFileRecordReader((CombineFileSplit) split, context, SmailRecordReader.class);
        }
    }

    /**
     * 重写分片规则
     */
    public static class SmailRecordReader extends RecordReader<Text, Text> {

        public Text key = new Text();
        public Text value = new Text();
        private boolean isFinish;//进度
        private CombineFileSplit split;
        private TaskAttemptContext context;
        private FSDataInputStream inputStream;
        private Integer currentIndex;//标记当前位置,必须为Integer

        //初始化--结构固定
        public SmailRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer currentIndex) {
            this.split = split;
            this.context = context;
            this.currentIndex = currentIndex;
        }

        public SmailRecordReader() {
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        }

        /*
         * 对key value赋值
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!isFinish) {
                //获取当前文件
                Path path = split.getPath(currentIndex);
                //获取文件名
                String fileName = path.getName();
                //key为文件名
                key.set(fileName);
                //创建当前文件文件系统
                FileSystem fs = path.getFileSystem(context.getConfiguration());
                //获取输入流
                inputStream = fs.open(path);
                //把整个文件装入数组
                byte[] content = new byte[(int)split.getLength(currentIndex)];
                //装入流
                inputStream.readFully(content);
                //将数组中文件内容装入value
                value.set(content);
                //防止无限执行
                isFinish = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return isFinish ? 1 : 0;//返回进度 1完成 0未完成
        }

        @Override
        public void close() throws IOException {

        }
    }

    public static class Test {
        public static class ForMapper extends Mapper<Text, Text, Text, Text> {
            @Override
            protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
                System.out.println(key.toString()+"-----"+value.toString());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //改配置为每一分片最大值，超过即再次分片
        configuration.setLong("mapreduce.input.fileinputformat.split.maxsize",409600);//400kb
        JobUtil.commitJob(Test.class, "E:\\File\\BigData\\TestData\\20180122\\computer", "", new SmailFileInputFormat(),configuration);
    }
}
