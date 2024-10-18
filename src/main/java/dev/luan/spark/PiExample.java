package dev.luan.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PiExample {
    public static void main(String... args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PiCalculator");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 1000000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        jsc.close();
    }
}