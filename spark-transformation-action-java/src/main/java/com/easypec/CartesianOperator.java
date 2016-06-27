package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jingtao on 2016/6/18.
 */
// 笛卡尔积操作
public class CartesianOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("cogroupOperator").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> clothes = Arrays.asList("T恤衫","夹克","皮大衣","衬衫","毛衣");
        List<String> trousers = Arrays.asList("西裤","内裤","铅笔裤","皮裤","牛仔裤");
        JavaRDD<String> clothesRDD = jsc.parallelize(clothes);
        JavaRDD<String> trousersRDD = jsc.parallelize(trousers);

        List<Tuple2<String, String>> list = clothesRDD.cartesian(trousersRDD).collect();

        for (Tuple2<String, String> tuple : list) {
            System.out.println(tuple);
        }

        jsc.close();
    }
}
