package exercise_1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercise_1 {

    public static String bankMarketing(JavaSparkContext spark) {
        String out = "";

        JavaRDD<String> bankRDD = spark.textFile("src/main/resources/bank.csv");

        JavaRDD<String[]> filterRDD = bankRDD.filter(f->!f.contains("age")).map(f->f.split(";")).cache();

        JavaRDD<String[]> noRDD = filterRDD.filter(f->f[7].equalsIgnoreCase("\"no\""));
        JavaRDD<String[]> yesRDD = filterRDD.filter(f->f[7].equalsIgnoreCase("\"yes\""));

        final JavaPairRDD<String, Tuple2<Integer, Integer>> noTupleRdd =  noRDD.mapToPair(record -> new Tuple2<>(record[1], new Tuple2<>(Integer.parseInt(record[5]), 1)));

        final JavaPairRDD<String, Tuple2<Integer, Integer>> yesTupleRdd =  yesRDD.mapToPair(record -> new Tuple2<>(record[1], new Tuple2<>(Integer.parseInt(record[5]), 1)));

        @SuppressWarnings("Duplicates")
        final JavaPairRDD<String, Double> noResultRdd = noTupleRdd.reduceByKey((tuple1, tuple2) -> {
            final Integer sum = tuple1._1 + tuple2._1;
            final Integer count = tuple2._2 + tuple1._2;
            return new Tuple2<>(sum, count);
        }).mapValues(tuple -> {
            final double sum = tuple._1;
            final int count = tuple._2;
            final double avg = sum / count;
            return avg;
        });

        @SuppressWarnings("Duplicates")
        final JavaPairRDD<String, Double> yesResultRdd = yesTupleRdd.reduceByKey((tuple1, tuple2) -> {
            final Integer sum = tuple1._1 + tuple2._1;
            final Integer count = tuple2._2 + tuple1._2;
            return new Tuple2<>(sum, count);
        }).mapValues(tuple -> {
            final double sum = tuple._1;
            final int count = tuple._2;
            final double avg = sum / count;
            return avg;
        });

        JavaPairRDD<String, Double> mapNoRDD = noResultRdd.mapToPair(f-> new Tuple2<>(f._1, f._2));
        JavaPairRDD<String, Double> mapYesRDD = yesResultRdd.mapToPair(f-> new Tuple2<>(f._1, f._2));
        JavaPairRDD<String, Tuple2<Double, Double>> mapRDD = mapNoRDD.join(mapYesRDD).sortByKey();

        out += mapRDD.map(f->{
            final String first = f._1;
            final String second = f._2._1.toString();
            final String third = f._2._2.toString();

            final String last = first + ": " + "avg balance with loan = " + third + ", without loan = " + second;
            return (last + "\n");

        }).collect();
        return out;
    }

}
