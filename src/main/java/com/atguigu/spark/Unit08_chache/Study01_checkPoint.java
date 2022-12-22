package com.atguigu.spark.Unit08_chache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author 姜来
 * @ClassName Study01_checkPoint.java
 * @createTime 2022年12月21日 15:24:00
 */
public class Study01_checkPoint {
    public static void main(String[] args) throws InterruptedException {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_checkPoint");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码
        // 1. 读取文件构建RDD==》 hello spark
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/word.txt");

        // TODO 使用sc对象设置checkPoint存储路径
        sc.setCheckpointDir("./doc/checkPoint");

        // 2. 炸裂  hello
        //         spark
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 3. 转换为kv类型RDD===>(hello,1)
        JavaPairRDD<String, Long> mapTOPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                //System.out.println("你看看我出现几次？");
                return new Tuple2<>(s, System.currentTimeMillis());
            }
        });

//        // TODO 查看mapTOPairRDD的血缘依赖
//        System.out.println("查看mapTOPairRDD的血缘依赖:");
//        System.out.println(mapTOPairRDD.toDebugString());

        // TODO　设置检查点
        mapTOPairRDD.cache();
        mapTOPairRDD.checkpoint();


        // 4. 收集打印
        mapTOPairRDD.collect().forEach(System.out::println);
//        // TODO 查看mapTOPairRDD的血缘依赖
//        System.out.println("查看mapTOPairRDD的血缘依赖:");
//        System.out.println(mapTOPairRDD.toDebugString());

        System.out.println("==============================");
        mapTOPairRDD.collect().forEach(System.out::println);


        Thread.sleep(100000);
        // TODO 关闭sc
        sc.stop();
    }
}
