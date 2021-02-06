package exercise_2_new;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Double.max;


public class Exercise_2_new {

    //The top 3 european richest countries according to their GDP per capita for 2015, 2016 and 2017
    public static String k_RichestCountries(JavaSparkContext spark, int K) {
        String out = "";

//        JavaRDD<String> report_2015 = spark.textFile("src/main/resources/2015.csv");
//        JavaRDD<String> report_2016 = spark.textFile("src/main/resources/2016.csv");
//        JavaRDD<String> report_2017 = spark.textFile("src/main/resources/2017.csv");
        JavaRDD<String> report_2015 = spark.textFile("src/main/resources/2015_long.csv");
        JavaRDD<String> report_2016 = spark.textFile("src/main/resources/2016_long.csv");
        JavaRDD<String> report_2017 = spark.textFile("src/main/resources/2017_long.csv");

        JavaRDD<String> unified = report_2015.union(report_2016).union(report_2017).repartition(1);

        List<Tuple2<String,Double>> ranking = unified.
                filter(t -> (!t.contains("Country") && t.contains("Europe")))
                .map(t-> t.split(","))
                .mapToPair(f -> new Tuple2<>(f[0], Double.parseDouble(f[5])))
                .reduceByKey((a,b)->max(a,b))
                .collect();
        Comparator<Tuple2<String,Double>> comp = (Tuple2<String,Double> a, Tuple2<String,Double> b) -> b._2.compareTo(a._2);
        List<Tuple2<String,Double>> ranking2;
        ranking2 = new ArrayList<>(ranking);
        ranking2.sort((a, b) -> Double.compare(b._2, a._2));
        List<String> topK = Lists.newArrayList();
        int i = 0;
        while (i < ranking.size() && topK.size() < K) {
            if (ranking.get(i) != null) {
                String country = ranking2.get(i)._1;
                if (!topK.contains(country)) {
                    topK.add(country);
                }
            }
            ++i;
        }
        out = topK.stream().collect(Collectors.joining("\n"));

        return out;
    }

}
