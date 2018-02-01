package com.oracle.forMR03;

import com.oracle.util.JobUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LHT on 2018/1/19
 */
public class ForJoinReduce {

    public static class Entity implements Writable {

        private String pk_orderId = "";
        private Long orderNum = 0L;
        private String date = "";
        private int num;
        private String name = "";
        private String type = "";
        private int price;
        private boolean flag;//true为order

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(pk_orderId);
            out.writeLong(orderNum);
            out.writeUTF(date);
            out.writeInt(num);
            out.writeUTF(name);
            out.writeUTF(type);
            out.writeInt(price);
            out.writeBoolean(flag);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.pk_orderId = in.readUTF();
            this.orderNum = in.readLong();
            this.date = in.readUTF();
            this.num = in.readInt();
            this.name = in.readUTF();
            this.type = in.readUTF();
            this.price = in.readInt();
            this.flag = in.readBoolean();
        }

        public boolean isFlag() {
            return flag;
        }

        /**
         * True -- order
         *
         * @param flag
         */
        public void setFlag(boolean flag) {
            this.flag = flag;
        }

        public String getPk_orderId() {
            return pk_orderId;
        }

        public void setPk_orderId(String pk_orderId) {
            this.pk_orderId = pk_orderId;
        }

        public Long getOrderNum() {
            return orderNum;
        }

        public void setOrderNum(Long orderNum) {
            this.orderNum = orderNum;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public int getNum() {
            return num;
        }

        public void setNum(int num) {
            this.num = num;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        @Override
        public String toString() {
            return "Entity{" +
                    "pk_orderId='" + pk_orderId + '\'' +
                    ", orderNum=" + orderNum +
                    ", date='" + date + '\'' +
                    ", num=" + num +
                    ", name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", price=" + price +
                    '}';
        }
    }

    public static class ForMapper extends Mapper<LongWritable, Text, Text, Entity> {

        private Text okey = new Text();
        private String fileName;
        private Entity entity = new Entity();

        /**
         * 执行map执行此方法，且只执行一次
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获得当前读到的个文件的文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();//获得文件分片
            fileName = fileSplit.getPath().getName();//获得文件名
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str = line.split("\t");
            if (str.length < 3) {//跳过无效数据
                return;
            }
            if (fileName.contains("order")) {
                okey.set(str[3]);
                entity.setOrderNum(Long.valueOf(str[0]));
                entity.setDate(str[1]);
                entity.setNum(Integer.parseInt(str[2]));
                entity.setPk_orderId(str[3]);
                entity.setFlag(true);
            } else if (fileName.contains("product")) {
                okey.set(str[0]);
                entity.setName(str[1]);
                entity.setType(str[2]);
                entity.setPrice(Integer.parseInt(str[3]));
                entity.setFlag(false);
            } else {
                try {
                    throw new Exception("file " + fileName + " is not we need !");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            context.write(okey, entity);
        }
    }

    public static class ForReducer extends Reducer<Text, Entity, Entity, NullWritable> {
        private Entity product = new Entity();

        @Override
        protected void reduce(Text key, Iterable<Entity> values, Context context) throws IOException, InterruptedException {
            List<Entity> list = new ArrayList<>();
            for (Entity e : values) {
                try {
                    if (e.isFlag()) {//order
                        /**
                         * 使用迭代器，返回的是指针，并非实体对象，添加入集合前需先存入实体对象
                         */
                        Entity order = new Entity();
                        BeanUtils.copyProperties(order, e);
                        list.add(order);
                    } else {
                        //复制相同属性的工具 (目标,源) Apache提供
                        BeanUtils.copyProperties(product, e);
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            for (Entity en : list) {
                en.setName(product.getName());
                en.setType(product.getType());
                en.setPrice(product.getPrice());
                context.write(en, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(ForJoinReduce.class, "E:\\File\\BigData\\TestData\\20180119\\jionData\\reduce", "");
    }
}
