package com.oracle.forMR03;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by LHT on 2018/1/19
 */
public class ForCombine {

    public static class Entity implements Writable {

        private int count;
        private int num;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(count);
            out.writeInt(num);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.count = in.readInt();
            this.num = in.readInt();
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public int getNum() {
            return num;
        }

        public void setNum(int num) {
            this.num = num;
        }

        @Override
        public String toString() {
            return "Entity{" +
                    "count=" + count +
                    ", num=" + num +
                    '}';
        }
    }

    public static class ForMapper extends Mapper<LongWritable, Text, Text, Entity> {
        private Text okey = new Text();
        private Entity en = new Entity();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str = line.split(" ");
            okey.set(str[0]);
            en.setNum(Integer.parseInt(str[1]));
            en.setCount(1);
            context.write(okey, en);
        }
    }

    public static class MyCombine extends Reducer<Text, Entity, Text, Entity> {
        private Entity ovlaue = new Entity();

        @Override
        protected void reduce(Text key, Iterable<Entity> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int num = 0;
            for (Entity e : values) {
                count += 1;
                num += e.getNum();
            }
            ovlaue.setNum(num);
            ovlaue.setCount(count);
            System.err.println("Count00=" + count);
            System.err.println("NUM00=" + num);
            context.write(key, ovlaue);
        }
    }

    public static class ForReducer extends Reducer<Text, Entity, Text, Entity> {
        private int count = 0;
        private int num = 0;
        private Entity ovalue = new Entity();

        @Override
        protected void reduce(Text key, Iterable<Entity> values, Context context) throws IOException, InterruptedException {
            for (Entity e : values) {
                count += e.getCount();
                num += e.getNum();
            }
            ovalue.setNum(num);
            ovalue.setCount(count);
            System.err.println("Count=" + count);
            System.err.println("NUM=" + num);
            context.write(key, ovalue);
        }
    }

    public static void main(String[] args) throws IOException {
        Job job = Job.getInstance();
        job.setCombinerClass(MyCombine.class);

        JobUtil.commitJob(ForCombine.class, "E:\\File\\BigData\\TestData\\20180119\\forCombiner", "", new MyCombine());
    }
}