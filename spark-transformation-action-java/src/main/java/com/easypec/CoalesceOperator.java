package com.easypec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jingtao on 2016/6/18.
 */
// 重新进行分区操作, 同repartition操作一样, repartition操作是coalesce操作的简写版
// 用于在filter之后, 避免数据倾斜, 重新进行分区.
// coalesce只能减少partition, 不能增加partition(不会改变, 分区的值)
// 但是reparation可以增加, 也可以减少分区.
public class CoalesceOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CoalesceOperator").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("xuruyun1","xuruyun2","xuruyun3"
                ,"xuruyun4","xuruyun5","xuruyun6"
                ,"xuruyun7","xuruyun8","xuruyun9"
                ,"xuruyun10","xuruyun11","xuruyun12");

       JavaRDD rdd1 =  jsc.parallelize(list, 4);
       List<String> res1 =  rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer partition, Iterator<String> iter) throws Exception {
                List<String> res = new ArrayList<String>();
                while (iter.hasNext()) {
                    res.add(partition + " -> " + iter.next());
                }
                return res.iterator();
            }
        }, true).collect();
        for (String str : res1) {
            System.out.println(str);
        }

        List<String> res2 = rdd1.coalesce(8).mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer partition, Iterator<String> iter) throws Exception {
                List<String> res = new ArrayList<String>();
                while (iter.hasNext()) {
                    res.add(partition + " -> " + iter.next());
                }
                return res.iterator();
            }
        }, true).collect();
        for (String str : res2) {
            System.out.println(str);
        }


        System.out.println();

        jsc.close();
    }
}
