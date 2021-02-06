import java.util.Arrays;

import exercise_1.Exercise_1;
import exercise_2.Exercise_2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        SparkConf conf = new SparkConf().setAppName("Lab5_Spark").setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("OFF");
        if (args.length < 1) {
            throw new Exception("Wrong number of parameters, usage: (exercise1,exercise2 k)");
        }

        if (args[0].equals("exercise1")) {
            System.out.println(Exercise_1.bankMarketing(ctx));
        } else if (args[0].equals("exercise2")) {
            Integer K = Integer.parseInt(args[1]);
            System.out.println(Exercise_2.k_RichestCountries(ctx, K));
        } else {
            throw new Exception("Wrong number of exercise");
        }
    }
}