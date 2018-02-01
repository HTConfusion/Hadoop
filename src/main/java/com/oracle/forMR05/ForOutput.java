package com.oracle.forMR05;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 输出至数据库
 * Created by LHT on 2018/1/27
 */
public class ForOutput {
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

        public static class MyOutputFormat extends FileOutputFormat<FlowOrder, NullWritable> {

            @Override
            public RecordWriter<FlowOrder, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
                return new MyRecordWriter();
            }
        }

        public static class MyRecordWriter extends RecordWriter<FlowOrder, NullWritable> {
            private static Connection CONN;
            private static PreparedStatement PS;

            static {
                try {
                    Class.forName("com.mysql.jdbc.Driver");
                    CONN = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "liyurun", "123456");
                    PS = CONN.prepareStatement("INSERT INTO tb_flow (phone_numberint, up_flow, down_flow, all_flow) VALUES (?,?,?,?)");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void write(FlowOrder key, NullWritable value) throws IOException, InterruptedException {
                try {
                    PS.setString(1, key.getPhoneNumber());
                    PS.setInt(2, key.getUpFlow());
                    PS.setInt(3, key.getDownFlow());
                    PS.setInt(4, key.getAllFlow());
                    PS.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                try {
                    PS.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
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
       
        Job job = Job.getInstance();
        job.setOutputFormatClass(ForPhoneOrder.MyOutputFormat.class);
        JobUtil.commitJob(ForPhoneOrder.class,"E:\\File\\BigData\\TestData\\20180118\\phone1.dat","",new ForPhoneOrder.MyOutputFormat());
    }
}
