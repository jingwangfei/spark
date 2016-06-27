package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jingtao on 2016/6/18.
 */
public class TakeOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReduceOperator")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 有一个集合，里面有1到10，10个数字，现在我们通过reduce来进行累加
        List<Integer> numberList = Arrays.asList(1, 4, 3, 2, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList, 3);

        List<Integer> top3Numbers = numbers.take(3);
        for(Integer num:top3Numbers){
            System.out.println(num);
        }

        sc.close();
    }
}
