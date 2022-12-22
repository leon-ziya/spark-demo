package com.atguigu.spark.Unit03_transcationOperator_KeyValue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study01_mapValues.java
 * @createTime 2022年12月20日 15:23:00
 */
public class Study01_mapValues {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_mapValues");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 创建kv类型RDD,对其中每个元素的value值拼接”`"
        // 1. 创建kv类型RDD
        JavaPairRDD<String, String> kvRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("k1", "v1"), new Tuple2<String, String>("k2", "v2"),
                new Tuple2<String, String>("k3", "v3"), new Tuple2<String, String>("k4", "v4")
        ));
        // 2. 对kvRDD中每个value值拼接"`"
        JavaPairRDD<String, String> mapValuesRDD = kvRDD.mapValues(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + "`";
            }
        });
        // 3. 收集打印
        mapValuesRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
