package com.zfm.hadoop.sequence;

import java.io.IOException;

public class SequenceMain {
    public static void main(String[] args) {
        SequenceOperation sequenceOperation = new SequenceOperation();
        if(args[0].equals("write")){
            try {
                sequenceOperation.write(args[1]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            try {
                sequenceOperation.read(args[1]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
