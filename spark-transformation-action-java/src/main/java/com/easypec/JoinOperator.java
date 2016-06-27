package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by jingtao on 2016/6/17.
 */
public class JoinOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("joinOperator").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, String> rdd1 = jsc.parallelizePairs(Arrays.asList(
                                            new Tuple2<Integer, String>(1, "jingtao"),
                                            new Tuple2<Integer, String>(1, "wangfei")));
        JavaPairRDD<Integer, String> rdd2 = jsc.parallelizePairs(Arrays.asList(
                                            new Tuple2<Integer, String>(1, "18"),
                                            new Tuple2<Integer, String>(1, "19")));

        JavaPairRDD<Integer, Tuple2<String, String>> joinRdd = rdd1.join(rdd2);
        joinRdd.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, String>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, String>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2);
            }
        });

        jsc.close();
    }
}
