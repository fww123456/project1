package core.rdd_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object operator_transform3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("c",3),("b",4)))

    sc.stop()
  }

}
/*
(0,1)
(0,2)
(1,3)
(1,4)
 */