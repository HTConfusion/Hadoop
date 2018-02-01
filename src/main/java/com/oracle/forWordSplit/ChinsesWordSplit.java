package com.oracle.forWordSplit;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by LHT on 2018/1/20
 */
public class ChinsesWordSplit {

    public static class WordSplit {
        public static class ForMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
            private Text okey = new Text();
            private Counter lineCounter;

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                lineCounter = context.getCounter("Counter","lineCount");
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                List<Word> wordList = new ArrayList<>();
                String line = value.toString();
                lineCounter.increment(1L);
                Pattern pattern = Pattern.compile("[\\u4e00-\\u9fa5A-Za-z]+");
                Matcher matcher = pattern.matcher(line);
                while (matcher.find()) {
                    String word = matcher.group();
                    System.err.println("word=" + word);
                    wordList = WordSegmenter.seg(word);
                }

                for (Word w : wordList) {
                    okey.set(w.toString());
                    context.write(okey, NullWritable.get());
                }

            }
        }

        public static class ForReducer extends Reducer<Text, NullWritable, Text, IntWritable> {
            private IntWritable ovalue = new IntWritable();

            @Override
            protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                for (NullWritable n : values) {
                    count++;
                }
                ovalue.set(count);
                context.write(key, ovalue);
            }
        }
    }

    public static class TopWord{
        public static class ForMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
            private IntWritable okey = new IntWritable();
            private Text ovalue = new Text();
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] str = line.split("\t");
                okey.set(Integer.parseInt(str[1]));
                ovalue.set(str[0]);
                context.write(okey,ovalue);
            }
        }

        public static class MyPartitioner extends Partitioner<IntWritable,Text>{
            @Override
            public int getPartition(IntWritable intWritable, Text text, int numPartitions) {
                Boolean isNotEnglist = text.toString().matches("^[a-zA-Z]*");
                if(isNotEnglist){
                    return 1;
                }else {
                    return 0;
                }
            }
        }
        public static class ForReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
            @Override
            protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for(Text t:values){
                    context.write(t,key);
                }
            }
        }
        public static class MyComparator extends IntWritable.Comparator{
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }

    public static void main(String[] args) {
        //JobUtil.commitJob(WordSplit.class, "E:\\File\\BigData\\TestData\\20180120", "");
        JobUtil.commitJob(TopWord.class, "E:\\File\\BigData\\TestData\\20180120\\1", "" ,new TopWord.MyComparator(),new TopWord.MyPartitioner(),2);

    }
}
