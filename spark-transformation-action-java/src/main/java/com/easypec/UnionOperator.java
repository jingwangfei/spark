package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by jingtao on 2016/6/17.
 */
public class UnionOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("unionOperator").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.parallelize(Arrays.asList("wangfei", "jingtao"))
                .union(jsc.parallelize(Arrays.asList("xiaolang", "nimeia")))
                .foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                });

        jsc.close();
    }
}
