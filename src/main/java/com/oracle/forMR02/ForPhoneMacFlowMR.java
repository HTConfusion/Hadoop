package com.oracle.forMR02;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by LHT on 2018/1/18
 */
public class ForPhoneMacFlowMR {
    public static class MacFlow implements Writable {

        private String mac;
        private int flow;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(mac);
            out.writeInt(flow);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.mac = in.readUTF();
            this.flow = in.readInt();
        }

        public String getMac() {
            return mac;
        }

        public void setMac(String mac) {
            this.mac = mac;
        }

        public int getFlow() {
            return flow;
        }

        public void setFlow(int flow) {
            this.flow = flow;
        }
    }

    public static class PhoneMacFlow {

        public static class ForMapper extends Mapper<LongWritable, Text, Text, MacFlow> {
            private Text okey = new Text();
            private MacFlow mf = new MacFlow();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] str = line.split("\t");
                int flow = Integer.parseInt(str[8]) + Integer.parseInt(str[9]);
                mf.setMac(str[2].split(":")[0]);
                mf.setFlow(flow);
                okey.set(str[1]);
                context.write(okey, mf);
            }
        }

        public static class ForReducer extends Reducer<Text, MacFlow, Text, Text> {
            private Text ovalue = new Text();

            @Override
            protected void reduce(Text key, Iterable<MacFlow> values, Context context) throws IOException, InterruptedException {
                Map<String, Integer> map = new HashMap<String, Integer>();

                for (MacFlow m : values) {
                    String mac = m.getMac();
                    Integer flow = m.getFlow();
                    if (map.containsKey(mac)) {
                        map.put(mac, map.get(mac) + flow);
                    } else {
                        map.put(mac, flow);
                    }
                }

                StringBuilder sb = new StringBuilder();
                for (Map.Entry entry : map.entrySet()) {
                    sb.append(entry.getKey() + ":" + entry.getValue() + "\t");
                }
                ovalue.set(sb.toString());
                context.write(key,ovalue);
            }
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(PhoneMacFlow.class,"E:\\File\\BigData\\TestData\\20180118\\phone1.dat","");
    }
}
