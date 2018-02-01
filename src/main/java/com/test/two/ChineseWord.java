package com.test.two;

import com.oracle.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * MapReduce输入——中文分词数据统计分析
 * Created by LHT on 2018/1/25
 */
public class ChineseWord {
    public static class One {
        public static class MyInputFormat extends CombineFileInputFormat<Text,Text>{
            @Override
            protected boolean isSplitable(JobContext context, Path filename) {
                return false;
            }

            @Override
            public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
                return new CombineFileRecordReader((CombineFileSplit) split,context,MyRecordeader.class);
            }
        }

        public static class MyRecordeader extends RecordReader<Text, Text>{

            private Text okey = new Text();
            private Text ovalue = new Text();
            private boolean isFinish;
            private CombineFileSplit split;
            private FSDataInputStream inputStream;
            private TaskAttemptContext context;
            private int currentIndex;

            public MyRecordeader(CombineFileSplit split, FSDataInputStream inputStream, int currentIndex) {
                this.split = split;
                this.inputStream = inputStream;
                this.currentIndex = currentIndex;
            }

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if(!isFinish){
                    String fileName = split.getPath(currentIndex).getName();
                    okey.set(fileName);

                    isFinish = true;
                    return true;
                }
                return false;
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return okey;
            }

            @Override
            public Text getCurrentValue() throws IOException, InterruptedException {
                return ovalue;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return isFinish?1:0;
            }

            @Override
            public void close() throws IOException {

            }
        }
        public static class ForMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                System.err.println(line);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setMapperClass(One.ForMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce输入——中文分词数据统计分析\\数据集及工具\\庖丁分词测试数据集 工具\\data\\data\\camera"),
                new Path("E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce输入——中文分词数据统计分析\\数据集及工具\\庖丁分词测试数据集 工具\\data\\data\\computer"),
                new Path("E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce输入——中文分词数据统计分析\\数据集及工具\\庖丁分词测试数据集 工具\\data\\data\\household"),
                new Path("E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce输入——中文分词数据统计分析\\数据集及工具\\庖丁分词测试数据集 工具\\data\\data\\mobile"),
                new Path("E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce输入——中文分词数据统计分析\\数据集及工具\\庖丁分词测试数据集 工具\\data\\data\\MP3"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\File\\BigData\\TestData\\Result\\outIn"));
        FileSystem fs = FileSystem.get(new URI("file:///E:/File/BigData/TestData/Result/outIn"), new Configuration());
        job.waitForCompletion(true);
        //JobUtil.commitJob(One.class, "E:\\File\\BigData\\Test\\MapReduce基础编程 练习题及答案\\MapReduce输入——中文分词数据统计分析\\数据集及工具\\庖丁分词测试数据集 工具\\data\\data", "");
    }
}
