package exercise_2;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class Exercise_2 {

    //The top 3 european richest countries according to their GDP per capita for 2015, 2016 and 2017
    public static String k_RichestCountries(JavaSparkContext spark, int K) {
        String out = "";

        JavaRDD<String> report_2015 = spark.textFile("src/main/resources/2015.csv");
        JavaRDD<String> report_2016 = spark.textFile("src/main/resources/2016.csv");
        JavaRDD<String> report_2017 = spark.textFile("src/main/resources/2017.csv");

        JavaRDD<String> unified = report_2015.union(report_2016).union(report_2017).repartition(1);

        List<Tuple2<Tuple2<Double, String>, Long>> ranking = unified.flatMap(t -> {
            if (t.contains("Country")) return new ArrayList<String>().iterator();
            else return Lists.newArrayList(t).iterator();
        })
                .mapToPair(t -> new Tuple2<>(Double.parseDouble(t.split(",")[5]), t))
                .groupByKey()
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._1, t._2.iterator().next()))
                .mapValues(t -> t.contains("Europe") ? t : null)
                .zipWithIndex() //Adds the position of each element in the RDD as a Long
                .collect();

        List<String> topK = Lists.newArrayList();
        int i = 0;
        while (i < ranking.size() && topK.size() < K) {
            if (ranking.get(i) != null) {
                String country = ranking.get(i)._1._2.split(",")[0];
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
