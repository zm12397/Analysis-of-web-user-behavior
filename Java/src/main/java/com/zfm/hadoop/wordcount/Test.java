package com.zfm.hadoop.wordcount;

import java.io.*;
import java.util.Random;

public class Test {
    public static void main(String[] args) throws IOException {
        File file = new File("E:\\Temp\\toCentOs\\words.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        String strs[] = {"china","my","lord","stack","java","python","hadoop"};
        for(int i = 0;i < 10000;i ++){
            Random random = new Random();
            String str = new String(strs[random.nextInt(6)] + " " + strs[random.nextInt(6)]);
            writer.write(str);
            writer.newLine();
        }
        writer.flush();
        writer.close();
    }
}
