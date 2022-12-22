package com.atguigu.spark.Unit01_CreateRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author 姜来
 * @ClassName Test01_createRddWithList.java
 * @createTime 2022年12月20日 09:58:00
 */
public class Test01_createRddWithList {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Test01_createRddWithList");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据集合构建RDD
        // 1. 创建一个集合
//        ArrayList<String> list = new ArrayList<>();
//        list.add("spark");

        // 2. 根据集合构建RDD
        //JavaRDD<String> javaRDD = sc.parallelize(list);
        JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("a", "b", "c", "d"));
        // 3. 收集打印
//        List<String> resultList = javaRDD.collect();
//        // 4. 遍历输出
//        for (String str : resultList) {
//            System.out.println(str);
//        }
        javaRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
