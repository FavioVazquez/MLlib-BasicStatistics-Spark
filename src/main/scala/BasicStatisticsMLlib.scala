import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by worker1 on 18/05/15.
 */
object BasicStatisticsMLlib {

def main(args: Array[String]) {

  val conf = new SparkConf()
      .setMaster("local")
//    .setMaster("mesos://master.mcbo.mood.com.ve:5050")
    .setAppName("Basic Statistics MLlib")
    .set("spark.executor.memory", "12g")
  val sc = new SparkContext(conf)

//  1. Summary Statistics

  /**
   * We provide column summary statistics for RDD[Vector] through the
   * function colStats available in Statistics.
   */

  /**
   * colStats() returns an instance of MultivariateStatisticalSummary,
   * which contains the column-wise max, min, mean, variance, and number
   * of nonzeros, as well as the total count.
   */

  val observations: RDD[Vector] = sc.parallelize((0 to 100).toSeq
  .map {i => Vectors.dense(i) } )

  // Compute column summary statistics.
  val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
  println(summary.mean) // a dense vector containing the mean for each column
  println(summary.variance) // column-wise variance
  println(summary.numNonzeros) //number of nonzeros in each column

  sc.stop()
  }
}

