import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by worker1 on 18/05/15.
 */
object BasicStatisticsMLlib {

def main(args: Array[String]) {

  val conf = new SparkConf()
    //  .setMaster("local")
    .setMaster("mesos://master.mcbo.mood.com.ve:5050")
    .setAppName("Basic Statistics MLlib")
    .set("spark.executor.memory", "12g")
  val sc = new SparkContext(conf)

  sc.stop()
  }
}

