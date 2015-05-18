import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
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

//  2. Correlations

  /**
   * Calculating the correlation between two series of data is a common
   * operation in Statistics. In MLlib we provide the flexibility to
   * calculate pairwise correlations among many series. The supported
   * correlation methods are currently Pearson’s and Spearman’s
   * correlation.
   */

  val seriesX: RDD[Double] = sc.parallelize(Seq.fill(1000)(math.random)
  . map { i => i})
  seriesX.take(20).foreach(println)

  val seriesY: RDD[Double] = sc.parallelize(Seq.fill(1000)(math.random)
  .map { i => i})
  seriesY.take(20).foreach(println)

    /**
     * compute the correlation using Pearson's method. Enter "spearman"
     * for Spearman's method. If a method is not specified,
     * Pearson's method will be used by default.
     */

  val correlation: Double = Statistics.corr(seriesX,seriesY,"pearson")
  println(correlation)

  val data: RDD[Vector] = sc.parallelize(Seq.fill(100)(math.random)
  .map {i => Vectors.dense(i)})
  data.take(20).foreach(println)

  /**
   * calculate the correlation matrix using Pearson's method. Use
   * "spearman" for Spearman's method. If a method is not specified,
   * Pearson's method will be used by default.
   */

  val correlMatrix: Matrix = Statistics.corr(data,"pearson")
  println(correlMatrix)

  // TODO: Finish tutorial with real data
  sc.stop()
  }
}

