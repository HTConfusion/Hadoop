package com.oracle.forMR02;

import com.oracle.util.JobUtil;
import com.oracle.util.MyJobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by LHT on 2018/1/18
 */
public class ForPhoneOrderMR {

    public static class FlowOrder implements WritableComparable<FlowOrder> {

        private String phoneNumber = "";
        private int upFlow = 0;
        private int downFlow = 0;
        private int allFlow;

        @Override
        public int compareTo(FlowOrder o) {
            if (o.allFlow == this.allFlow) {
                return o.upFlow - this.upFlow;
            }
            return o.allFlow - this.allFlow;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(phoneNumber);
            out.writeInt(upFlow);
            out.writeInt(downFlow);
            out.writeInt(allFlow);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.phoneNumber = in.readUTF();
            this.upFlow = in.readInt();
            this.downFlow = in.readInt();
            this.allFlow = in.readInt();
        }

        public String getPhoneNumber() {
            return phoneNumber;
        }

        public void setPhoneNumber(String phoneNumber) {
            this.phoneNumber = phoneNumber;
        }

        public int getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(int upFlow) {
            this.upFlow = upFlow;
        }

        public int getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(int downFlow) {
            this.downFlow = downFlow;
        }

        public int getAllFlow() {
            this.allFlow = this.upFlow + this.downFlow;
            return this.allFlow;
        }

        public void setAllFlow(int allFlow) {
            this.allFlow = allFlow;
        }

        @Override
        public String toString() {
            return phoneNumber + "\t" + upFlow + "\t" + downFlow + "\t" + allFlow;
        }
    }

    public static class ForPhoneOrder {

        public static class ForMapper extends Mapper<LongWritable, Text, Text, FlowOrder> {
            private FlowOrder fo = new FlowOrder();
            private Text okey = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] str = line.split("\t");
                okey.set(str[1]);
                fo.setUpFlow(Integer.parseInt(str[8]));
                fo.setDownFlow(Integer.parseInt(str[9]));
                context.write(okey, fo);
            }
        }

        public static class MyPartitioner extends Partitioner<Text,FlowOrder>{//泛型为map输出

            @Override
            public int getPartition(Text text, FlowOrder flowOrder, int numPartitions) {
                String phoneNumber = text.toString().substring(0,3);//左闭右开
                if("135".equals(phoneNumber)){
                    return 1;
                }
                if("136".equals(phoneNumber)){
                    return 2;
                }
                if("137".equals(phoneNumber)){
                    return 3;
                }
                if("139".equals(phoneNumber)){
                    return 4;
                }
                return 0;
            }
        }

        public static class ForReducer extends Reducer<Text, FlowOrder, FlowOrder, NullWritable> {
            private FlowOrder fo = new FlowOrder();

            @Override
            protected void reduce(Text key, Iterable<FlowOrder> values, Context context) throws IOException, InterruptedException {
                fo.setPhoneNumber(key.toString());
                for (FlowOrder f : values) {
                    fo.setUpFlow(fo.getUpFlow() + f.getUpFlow());
                    fo.setDownFlow(fo.getDownFlow() + f.getDownFlow());
                }
                fo.getAllFlow();
                context.write(fo, NullWritable.get());
            }
        }
    }

    public static class ForPhoneOrderTwo {

        public static class ForMapper extends Mapper<LongWritable, Text, FlowOrder, NullWritable> {
            private FlowOrder fo = new FlowOrder();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] str = line.split("\t");
                fo.setPhoneNumber(str[0]);
                fo.setUpFlow(Integer.parseInt(str[1]));
                fo.setDownFlow(Integer.parseInt(str[2]));
                fo.getAllFlow();
                context.write(fo, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        /*--ForPhoneOrder--*/
          //JobUtil.commitJob(ForPhoneOrder.class, "E:\\File\\BigData\\TestData\\20180118\\phone1.dat", "");
        /*--ForPhoneOrderTwo--*/
          //JobUtil.commitJob(ForPhoneOrderTwo.class, "E:\\File\\BigData\\TestData\\20180118\\phone2.txt", "");
        /*--Partitioner--*/
          Job job = Job.getInstance();
          job.setPartitionerClass(ForPhoneOrder.MyPartitioner.class);
          job.setNumReduceTasks(5);
          job.setMapperClass(ForPhoneOrder.ForMapper.class);
          job.setReducerClass(ForPhoneOrder.ForReducer.class);
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(FlowOrder.class);
          job.setOutputKeyClass(FlowOrder.class);
          job.setOutputValueClass(NullWritable.class);
          MyJobUtil.jobPathClear(job,"E:\\File\\BigData\\TestData\\20180118\\phone1.dat","");
    }
}