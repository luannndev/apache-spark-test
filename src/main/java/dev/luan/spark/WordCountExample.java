package dev.luan.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountExample {

    public static void main(String[] args) {
        new WordCountExample().countWords("./test.txt");
    }

    private void countWords(String filename) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCounter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputLines = sc.textFile(filename);

        JavaRDD<String> words = inputLines.flatMap(s -> {
            return Arrays.asList(s.trim().split(" ")).iterator();
        });

        JavaRDD<String> punctuationRemovedWords = words.map(w -> {
            return w.replaceAll(",|\\.|\\(|\\)|;|:", "");
        });

        JavaRDD<String> stopWordRemovedWords = punctuationRemovedWords.filter(s -> {
            return !s.equalsIgnoreCase("the");
        });

        long cnt = words.count();
        long cnt2 = punctuationRemovedWords.count();
        long cnt3 = stopWordRemovedWords.count();
        System.out.println("Total words : " + cnt);
        System.out.println("Words without puncutation: " + cnt2);
        System.out.println("Total words without stopwords: " + cnt3);

        JavaPairRDD<String, Long> wordCounts = stopWordRemovedWords.mapToPair(s -> new Tuple2<>(s, 1l)).reduceByKey((s1, s2) -> s1 + s2);
        JavaPairRDD<String, Long> wordCountSorted = wordCounts.mapToPair(i -> i.swap()).sortByKey(false, 1).mapToPair(i2 -> i2.swap());

        List<Tuple2<String, Long>> top10 = wordCountSorted.take(10);


        top10.forEach(w -> System.out.printf("%s: %d\n", w._1(), w._2()));
        sc.close();

    }
}