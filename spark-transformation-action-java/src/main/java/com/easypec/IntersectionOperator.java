package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jingtao on 2016/6/17.
 */
public class IntersectionOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("intersection").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list1 = Arrays.asList("wangfei", "jingtao");
        List<String> list2 = Arrays.asList("wangfei", "xiaolang");
        JavaRDD<String> rdd1 = jsc.parallelize(list1);
        JavaRDD<String> rdd2 = jsc.parallelize(list2);

        rdd1.intersection(rdd2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        jsc.close();
    }
}
