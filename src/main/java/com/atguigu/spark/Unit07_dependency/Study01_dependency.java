package com.atguigu.spark.Unit07_dependency;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author 姜来
 * @ClassName Study01_dependency.java
 * @createTime 2022年12月21日 14:22:00
 */
public class Study01_dependency {
    public static void main(String[] args) throws InterruptedException {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_dependency");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 计算单词出现次数
        // 1. 读取文件构建RDD==》 hello spark
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/word.txt");

        // TODO 查看javaRDD的血缘依赖
        System.out.println("查看javaRDD的血缘依赖:");
        System.out.println(javaRDD.toDebugString());

        // 2. 炸裂  hello
        //         spark
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // TODO 查看jflatMapRDD的血缘依赖
        System.out.println("查看flatMapRDD的血缘依赖:");
        System.out.println(flatMapRDD.toDebugString());

        // 3. 转换为kv类型RDD===>(hello,1)
        JavaPairRDD<String, Integer> mapTOPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // TODO 查看mapTOPairRDD的血缘依赖
        System.out.println("查看mapTOPairRDD的血缘依赖:");
        System.out.println(mapTOPairRDD.toDebugString());

        // 4. 统计单词个数
        JavaPairRDD<String, Integer> reduceByeKeyRDD = mapTOPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // TODO 查看reduceByeKeyRDD的血缘依赖
        System.out.println("查看reduceByeKeyRDD的血缘依赖:");
        System.out.println(reduceByeKeyRDD.toDebugString());

        // 4. 收集打印
        reduceByeKeyRDD.collect().forEach(System.out::println);
        reduceByeKeyRDD.collect().forEach(System.out::println);


        Thread.sleep(100000);
        // TODO 关闭sc
        sc.stop();
    }
}
