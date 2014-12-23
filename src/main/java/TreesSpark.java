import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class TreesSpark {

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    //Calculates the average height of the trees in Paris
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Trees").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<Double> heights = ctx.textFile("./src/main/resources/les-arbres.csv", 1) //reading source file
                .filter(s -> !s.startsWith("geo")) //forget about the header
                .map(line->line.split(";")) //split fields
                .map(fields -> Double.valueOf(fields[4])) //read heights
                .filter(h -> h > 0); // keep only positive values
        System.out.println("Average tree size : "+heights.reduce(new Sum())/heights.count());

        ctx.stop();

    }
}
