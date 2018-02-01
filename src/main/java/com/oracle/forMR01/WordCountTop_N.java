package com.oracle.forMR01;

import com.oracle.util.MyJobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WordCountTop_N {

    //mapreduce自定义数据类型
    public static class WordCountEntity implements WritableComparable<WordCountEntity> {

        private String word;
        private int times;

        public int compareTo(WordCountEntity o) {
            return o.times - this.times;//倒序
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(word);
            dataOutput.writeInt(times);

        }

        public void readFields(DataInput dataInput) throws IOException {

            this.word = dataInput.readUTF();
            this.times = dataInput.readInt();
        }

        public WordCountEntity() {
        }

        public WordCountEntity(String word, int times) {
            this.word = word;
            this.times = times;
        }


        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getTimes() {
            return times;
        }

        public void setTimes(int times) {
            this.times = times;
        }

        @Override
        public String toString() {
            return "WordCountEntity{" +
                    "word='" + word + '\'' +
                    ", times=" + times +
                    '}';
        }

    }

    public static class ForMapper extends Mapper<LongWritable, Text, WordCountEntity, NullWritable> {

        List<WordCountEntity> list = new ArrayList<WordCountEntity>(11);
        {
            for(int i=0;i<10.;i++){
                list.add(new WordCountEntity());
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str = line.split("\t");

            list.add(new WordCountEntity(str[0], Integer.parseInt(str[1])));
            Collections.sort(list);
            list.remove(10);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i =0;i<10;i++){
                context.write(list.get(i),NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(WordCountEntity.class);
        job.setMapOutputValueClass(NullWritable.class);

        MyJobUtil.jobPathClear(job,"E:\\File\\BigData\\TestData\\Result\\part-r-00000","");
    }
}
