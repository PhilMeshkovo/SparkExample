import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Loader {
  private static final Pattern SPACE = Pattern.compile("\\s+");
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]").set("spark.driver.host", "localhost");
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> strings = context
        .textFile(args[0]);
    JavaRDD<String> words = strings.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
    JavaPairRDD<String,Integer> ones = words.mapToPair(s -> new Tuple2<>(s,1));
    JavaPairRDD<String,Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    counts.saveAsTextFile(args[1]);
    context.stop();
  }
}
