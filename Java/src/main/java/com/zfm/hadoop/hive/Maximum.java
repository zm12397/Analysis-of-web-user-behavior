package com.zfm.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class Maximum extends UDAF{
    public static class MaximumIntUDAFEvaluator implements UDAFEvaluator{
        private IntWritable result;

        /**
         * 初始化
         * 状态重置
         */
        public void init() {
            result = null;
        }

        /**
         * 迭代
         * 比较，选出最大值，保存状态
         * @param value
         * @return 输入格式正确则返回true
         */
        public boolean iterate(IntWritable value){
            if(value == null){

            }else{
                if (result == null){
                    result = new IntWritable(value.get());
                }else{
                    result.set(Math.max(result.get(),value.get()));
                }
            }
            return true;
        }

        /**
         * 返回部分聚集结果
         * @return
         */
        public IntWritable terminatePartial(){
            return result;
        }

        /**
         * 部分聚集直接合并
         * @param other 其他部分聚集的值
         * @return
         */
        public boolean merge(IntWritable other){
            return iterate(other);
        }

        /**
         * 返回最终结果
         * @return
         */
        public IntWritable terminate(){
            return result;
        }
    }
}
