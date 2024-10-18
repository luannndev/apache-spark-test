package dev.luan.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CharCountExample {

    public static void main(String[] args) {
        new CharCountExample().charCount("./test.txt");
    }

    private void charCount(String filename) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Charcounter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(filename);
        JavaRDD<String> chars = input.flatMap(s -> Arrays.asList(s.split("")).iterator());
        JavaPairRDD<String, Integer> counts = chars.mapToPair(t -> new Tuple2<>(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        counts = counts.mapToPair(item -> item.swap()).sortByKey(false, 1).mapToPair(item -> item.swap());

        List<Tuple2<String, Integer>> list = counts.take(10);

        for (Tuple2<String, Integer> t : list) {
            System.out.printf("'%s' - %d\n", t._1(), t._2());
        }

        System.out.println(counts);
        sc.close();
    }
}