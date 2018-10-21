package com.zfm.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

public class HDFSOperator {
    private FileSystem fs = null;
    private static String url = "hdfs://master:9000/";

    public void init() throws IOException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(url),conf);
    }

    public void uploadFile(Path source, Path target) throws IOException {
        fs.copyFromLocalFile(source,target);
    }

    public void downloadFile(Path source, Path target) throws IOException {
        fs.copyToLocalFile(source,target);
    }

    //创建目录，包括不存在的父目录
    public void mkdir(Path dir) throws IOException {
        fs.mkdirs(dir);
    }

    //删除目录，布尔为true时删除非空目录
    public void rmdir(Path dir,boolean iteration) throws IOException {
        fs.delete(dir,iteration);
    }

    public void rename(Path source,Path target) throws IOException {
        fs.rename(source,target);
    }

    public boolean check(Path path) throws IOException {
        return fs.exists(path);
    }

    public void ls(Path path) throws IOException {
        FileStatus[] files = fs.listStatus(path);
        Path[] listedPaths = FileUtil.stat2Paths(files);
        for(Path p : listedPaths){
            System.out.println(p);
        }
    }

    //文件详细信息，包括文件名，块数量，权限，长度，每个块的详细信息
    public void listFileMetadata(Path path) throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path,true);
        while(files.hasNext()){
            LocatedFileStatus fileStatus = files.next();
            System.out.println(FileDescriptor.desc(fileStatus));
        }
    }


}
