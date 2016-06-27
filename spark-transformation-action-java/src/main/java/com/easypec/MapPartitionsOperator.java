package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jingtao on 2016/6/17.
 */
public class MapPartitionsOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        List<String> list = Arrays.asList("jingtao", "wangfei", "xiaolang");
        JavaRDD<String> elems = jsc.parallelize(list);

        JavaRDD<String> hahaElems = elems.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> stringIterator) throws Exception {
                List<String> res = new ArrayList<String>();
                while(stringIterator.hasNext()) {
                    res.add(stringIterator.next() + "-haha");
                }
                return res;
            }
        });

        hahaElems.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        jsc.close();
    }
}
