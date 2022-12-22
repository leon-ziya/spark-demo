package com.atguigu.spark.Unit05_WordCountCase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author 姜来
 * @ClassName Study01_wordCount.java
 * @createTime 2022年12月21日 09:54:00
 */
public class Study01_wordCount {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_wordCount");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 统计文件中单词出现次数
        // 1. 根据文件构建RDD  ===> hello spark
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/word.txt");
        // 2. 炸裂 hello spark  ===> hello
        //                          spark
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        // 3. 将value类型RDD转换为kvRDD====》 mapToPair ===> (hello,1)
        JavaPairRDD<String, Integer> mapToPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        // 4. 使用reduceByKey 实现相同key的value值聚合 ===> (hello,2)
        JavaPairRDD<String, Integer> reduceByKeyRDD = mapToPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 5. 收集打印
        reduceByKeyRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
