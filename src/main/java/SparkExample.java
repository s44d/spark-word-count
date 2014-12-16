import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class SparkExample {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        //Read from an external file
        JavaRDD<String> lines = ctx.textFile("./src/main/resources/pulpfiction.txt", 1);

        //split into words
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                s = s.replaceAll("[^a-zA-Z]]"," ").toLowerCase();
                s = s.replaceAll("\\p{P}"," ");
                return Arrays.asList(SPACE.split(s));
            }
        });

        //map with key-value pairs <word,count>
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2(s, 1));

        //reduce by Key (word) : count occurences. And sort by Key values (alphabetic order).
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((a, b) -> a + b).sortByKey();

        //display values
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }
}
