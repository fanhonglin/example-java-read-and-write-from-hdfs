package io.saagie.example.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.URI;
import java.util.logging.Logger;


public class MainKerberos {

    private static final Logger logger = Logger.getLogger("io.saagie.example.hdfs.MainKerberos");

    public static void main(String[] args) throws Exception {

        // 用户
        String kerUser = "hdfs";

        String krb5Path = "/data/krb5.conf";
        String keyTabPath = "/data/hdfs.keytab";

        // hdfs url
        String hdfsUrl = "hdfs://10.40.7.110:8020";

        // hdfs path
        String path = "/user/hdfs/example/hdfs/";

        String fileName = "hello.csv";
        String fileContent = "hello;world";

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // kerberos
        conf.set("hadoop.security.authentication", "Kerberos");

        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", kerUser);
        System.setProperty("hadoop.home.dir", "/");
        // krb5.conf
        System.setProperty("java.security.krb5.conf", krb5Path);

        try {
            UserGroupInformation.setConfiguration(conf);
            //对应kerberos principal的keytab文件,从服务器获取放置本地
            UserGroupInformation.loginUserFromKeytab(kerUser, keyTabPath);
            logger.info("login success ！");
        } catch (Exception e) {
            logger.info("Kerbers认证异常 ！" + e.getMessage());
        }

        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);

        //==== Create folder if not exists
        Path workingDir = fs.getWorkingDirectory();
        Path newFolderPath = new Path(path);
        if (!fs.exists(newFolderPath)) {
            // Create new Directory
            fs.mkdirs(newFolderPath);
            logger.info("Path " + path + " created.");
        }

        //==== Write file
        logger.info("Begin Write file into hdfs");
        //Create a path
        Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
        //Init output stream
        FSDataOutputStream outputStream = fs.create(hdfswritepath);
        //Cassical output stream usage
        outputStream.writeBytes(fileContent);
        outputStream.close();
        logger.info("End Write file into hdfs");

        //==== Read file
        logger.info("Read file into hdfs");
        //Create a path
        Path hdfsreadpath = new Path(newFolderPath + "/" + fileName);
        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        //Classical input stream usage
        String out = IOUtils.toString(inputStream, "UTF-8");
        logger.info(out);
        inputStream.close();
        fs.close();

    }
}
