package com.zfm.hadoop.hdfs;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        HDFSOperator hdfs = new HDFSOperator();
        HDFSStreamAccess stream = new HDFSStreamAccess();
        try {
            hdfs.init();
            stream.init();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("操作类初始化失败：" + e.getMessage());
            System.exit(1);
        }
        if(args[0] .equals("uploadFile")) {
            try {
                hdfs.uploadFile(new Path(args[1]), new Path(args[2]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("downloadFile")) {
            try {
                hdfs.downloadFile(new Path(args[1]), new Path(args[2]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("mkdir")) {
            try {
                hdfs.mkdir(new Path(args[1]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("rmdir")) {
            try {
                hdfs.rmdir(new Path(args[1]),Boolean.parseBoolean(args[2]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("rename")) {
            try {
                hdfs.rename(new Path(args[1]), new Path(args[2]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("check")) {
            boolean flag = false;
            try {
                flag = hdfs.check(new Path(args[1]));
                System.out.println(flag);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("ls")) {
            try {
                hdfs.ls(new Path(args[1]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("listFileMetadata")) {
            try {
                hdfs.listFileMetadata(new Path(args[1]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(args[0] .equals("read")) {
            if(args.length == 2){
                stream.read(args[1]);
            }else if(args.length == 3){
                stream.read(args[1],args[2]);
            }
        }else if(args[0] .equals("write")) {
            stream.write(args[1],args[2]);
        }else if(args[0] .equals("append")) {
            stream.append(args[1],args[2]);
        }else if(args[0].equals("readBlock")){
            stream.readBlock(args[1],args[2]);
        }
    }
}
