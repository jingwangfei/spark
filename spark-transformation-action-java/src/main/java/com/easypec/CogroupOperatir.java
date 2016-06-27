package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by jingtao on 2016/6/18.
 */

// 先对每个rdd中的元素进行分组操作, 然后在对2个rdd进行join联合操作
// 最重要的是同join的區別是, 在進行一對一, 一對多, 多對多操作的時候, 只會返回一行記錄
// 同時, 它也可以对3个RDD进行操作
public class CogroupOperatir {

    public static void main(final String[] args) {
        SparkConf conf = new SparkConf().setAppName("cogroupOperator").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> orders = jsc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<String, String>("product1", "order1"),
                        new Tuple2<String, String>("product1", "order2"))
        );

        JavaPairRDD<String, String> products = jsc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<String, String>("product1", "gangbi"),
                        new Tuple2<String, String>("product1", "zhi"))
        );

        orders.cogroup(products).foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple) throws Exception {
                System.out.println(tuple._1);
                System.out.println(tuple._2);
            }
        });

        jsc.close();
    }
}
