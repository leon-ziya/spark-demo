package com.atguigu.spark.Unit01_CreateRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author 姜来
 * @ClassName Test.java
 * @createTime 2022年12月20日 09:23:00
 */
public class Test {
    public static void main(String[] args) {
    // TODO 创建SparkConf
    SparkConf sparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("Test");


    // TODO 创建JavaSparkContext
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // TODO 业务逻辑代码

    // TODO 关闭sc
    sc.stop();
    }
}
