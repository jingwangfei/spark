package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jingtao on 2016/6/18.
 */
public class AggregateByKeyOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("cogroupOperator").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);



        jsc.close();
    }
}
