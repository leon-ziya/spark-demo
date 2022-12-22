package com.atguigu.spark.Unit08_chache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author 姜来
 * @ClassName Study01_cache.java
 * @createTime 2022年12月21日 15:24:00
 */
public class Study01_cache {
    public static void main(String[] args) throws InterruptedException {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_cache");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码
        // 1. 读取文件构建RDD==》 hello spark
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/word.txt");

        // 2. 炸裂  hello
        //         spark
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 3. 转换为kv类型RDD===>(hello,1)
        JavaPairRDD<String, Integer> mapTOPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                System.out.println("你看看我出现几次？");
                return new Tuple2<>(s, 1);
            }
        });

        // TODO 查看mapTOPairRDD的血缘依赖
        System.out.println("查看mapTOPairRDD的血缘依赖:");
        System.out.println(mapTOPairRDD.toDebugString());

        //mapTOPairRDD.cache();
        mapTOPairRDD.persist(StorageLevel.MEMORY_ONLY());


        // 4. 收集打印
        mapTOPairRDD.collect().forEach(System.out::println);
        // TODO 查看mapTOPairRDD的血缘依赖
        System.out.println("查看mapTOPairRDD的血缘依赖:");
        System.out.println(mapTOPairRDD.toDebugString());
        mapTOPairRDD.collect().forEach(System.out::println);


        Thread.sleep(100000);
        // TODO 关闭sc
        sc.stop();
    }
}
