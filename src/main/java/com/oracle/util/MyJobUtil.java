package com.oracle.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @author LHT
 */
public class MyJobUtil {

    public static void jobPathClear(Job job,String pathIN,String pathOut){
        if(pathOut==null||"".equals(pathOut)){
            pathOut = "E:/File/BigData/TestData/Result/";
        }else{
            pathOut = "E:/File/BigData/TestData/Result/"+pathOut;
        }
        Path path = new Path(pathOut);
        try {
            FileSystem fs = FileSystem.get(new URI("file:///"+pathOut),new Configuration());
            if(fs.exists(path)){
                fs.delete(path,true);
            }
            FileInputFormat.setInputPaths(job,new Path(pathIN));
            FileOutputFormat.setOutputPath(job,new Path(pathOut));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
