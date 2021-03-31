package core.rdd_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //withReplacement：抽取是否放回
    //fraction：每个数被抽取的gail
    //seed：种子
    println(rdd.sample(
      withReplacement = false,
      fraction = 0.5,
      seed = 1
    ).collect().mkString(","))

    sc.stop()
  }

}
