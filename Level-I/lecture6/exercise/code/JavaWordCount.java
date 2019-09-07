import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class JavaWordCount {
  public static void main(String[] args) {

    // create Spark context with Spark configuration
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));

    // read in text file and split each document into words
    JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(
      new FlatMapFunction() {
        public Iterable call(String s) {
          return Arrays.asList(s.split(" "));
        }
      }
    );

    // count the occurrence of each word
    JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
      new PairFunction() {
        public Tuple2 call(String s) {
          return new Tuple2(s, 1);
        }
      }
    ).reduceByKey(
      new Function2() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }
    );
	//print the result
    System.out.println(counts.collect());
  }
}