package com.zfm.hadoop.hdfs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;

import java.io.IOException;

public class FileDescriptor {
    public static String desc(LocatedFileStatus fileStatus){
        BlockLocation[] blockLocations = fileStatus.getBlockLocations();
        String description = "name:\t" + fileStatus.getPath().getName() + "\n" +
                "blockSize:\t" + fileStatus.getBlockSize() + "\n" +
                "permission:\t" + fileStatus.getPermission() + "\n" +
                "len:\t" + fileStatus.getLen() + "\n";
        String str_block = "";
        for(BlockLocation block : blockLocations){
            long block_length = block.getLength();
            long block_offset = block.getOffset();
            String[] hosts = null;
            String str_hosts = "";
            try {
                hosts = block.getHosts();
                for(String host : hosts){
                    str_hosts += host + "\n";
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            str_block += "block_length:\t" + block_length + "\n" +
                    "block_offset:\t" + block_offset + "\n" +
                    "str_hosts:\n" + str_hosts;

        }
        return description + str_block;
    }
}
