package com.oracle.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author LHT
 * Hadoop-HDFS-Java-Api
 */
public class TestDFS {

    private FileSystem fs;

    //master的url
    private String url = "hdfs://192.168.44.129:8020";

    public void setUp() throws URISyntaxException, IOException, InterruptedException {//方法名无所谓
        fs = FileSystem.get(new URI(url), new Configuration(), "LHT");
        System.out.println(fs);
    }

    public void testMkDir() throws IOException {//创建文件夹
        fs.mkdirs(new Path("/forCL"));
    }

    public void testCreatFile() throws IOException {//创建文件并写入
        FSDataOutputStream fso = fs.create(new Path("/forCL/test.txt"));
        for (int i = 0; i < 3; i++) {
            fso.write(("helloHadoop" + i + "\n").getBytes());
        }
        fso.flush();
        fso.close();
    }

    public void testCatFile() throws IOException {//读取文件
        FSDataInputStream fsi = fs.open(new Path("/forCL/test.txt"));
        byte[] temp = new byte[512];
        int len = fsi.read(temp);
        while (len > 0) {
            System.out.println(new String(temp, 0, len));
            len = fsi.read(temp);
        }
    }

    public void testRename() throws IOException {//重命名&移动目录
        fs.rename(new Path("/forCL/test.txt"), new Path("/forCL/rename.txt"));
    }

    public void remove() throws IOException {//删除文件&目录
        //boolean:级联删除 文件夹：true 文件：false
        fs.delete(new Path("/forCL/rename.txt"), false);
    }

    public void upload() throws IOException {//上传-可重命名
        fs.copyFromLocalFile(new Path("E://xwj"), new Path("/forCL/"));
    }

    public void downLoad() throws IOException {//下载文件-可重命名
        //fs.copyToLocalFile(new Path("/forCL/test.xlsx"),new Path("E://aa.xlsx"));//空指针异常,需配置window hadoop环境
        fs.copyToLocalFile(false, new Path("/forCL/test.xlsx"), new Path("E://aa.xlsx"), true);
    }

    public void testInfo() throws IOException {//查看空间属性
        FsStatus fss = fs.getStatus();
        System.out.println(fss.getUsed());//已使用的
        System.out.println(fss.getCapacity());//容量
        System.out.println(fss.getRemaining());//剩余容量
    }

    public void testInfoA() throws IOException {//查看文件属性
        FileStatus fsas = fs.getFileStatus(new Path("/forCL/test.xlsx"));
        System.out.println(fsas.getAccessTime());
        System.out.println(fsas.getOwner());
        System.out.println(fsas.getReplication());//备份数
        System.out.println(fsas.getBlockSize());
        System.out.println(fsas.getPath());
    }

    public void testDS() throws IOException {//获取数据节点信息
        DistributedFileSystem ds = (DistributedFileSystem) fs;
        DatanodeInfo[] dni = ds.getDataNodeStats();
        for (DatanodeInfo di : dni) {
            System.out.println(di);
        }
    }

    public void mergeFile() throws IOException {//合并小文件
        //源文件系统，源文件，目的文件系统，目的路径，是否删除源文件，配置，添加字符
        FileUtil.copyMerge(fs, new Path("/forCL/xwj"), fs, new Path("/forCL/xwjM"), false, new Configuration(), "");
    }

    public void tearDown() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
