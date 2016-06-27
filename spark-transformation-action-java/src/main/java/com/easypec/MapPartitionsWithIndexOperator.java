package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jingtao on 2016/6/17.
 */
public class MapPartitionsWithIndexOperator {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("jingtao", "wanffei", "nimeia");
        JavaRDD<String> rdd = jsc.parallelize(list, 3);

        JavaRDD<String> mapRdd = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer partition, Iterator<String> iter) throws Exception {
                List<String> res = new ArrayList<String>();
                while(iter.hasNext()) {
                    res.add(partition + "," + iter.next());
                }
                return res.iterator();
            }
        }, true);

        mapRdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        jsc.close();
    }

}
