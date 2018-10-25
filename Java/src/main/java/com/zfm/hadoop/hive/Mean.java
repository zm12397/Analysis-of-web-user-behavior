package com.zfm.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
//import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class Mean extends UDAF {
    public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator{

        public static class PartialResult{
            double sum;
            long count;
        }
        private PartialResult partialResult;

        public void init() {
            partialResult = null;
        }

        /**
         * 这里不能用 DoubleWritable
         * @param value
         * @return
         */
        public boolean iterate(Double value){
            if(value == null){

            }else{
               if(partialResult == null) {
                   partialResult = new PartialResult();
               }
               partialResult.count ++;
               partialResult.sum += value;
            }
            return true;
        }

        public PartialResult terminatePartial(){
            return partialResult;
        }

        public boolean merge(PartialResult other){
            if(other == null){

            }else {
                if(partialResult == null){
                    partialResult = new PartialResult();
                }
                partialResult.count += other.count;
                partialResult.sum += other.sum;
            }
            return true;
        }

        public Double terminate(){
            if (partialResult == null){
                return null;
            }
            return new Double(partialResult.sum / partialResult.count);
        }
    }
}
