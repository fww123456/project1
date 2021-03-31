package core.rdd_operator

import org.apache.spark.{SparkConf, SparkContext}

object operator_transform4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4),numSlices = 2)
    //[1,2],[3,4]-->[2],[4]-->[6]
    //首先转化成数组
    val glomRDD = rdd.glom()

    //取数
    val maxRDD = glomRDD.map(
      array=>{
        array.max
      }
    )

    println(maxRDD.collect().sum)
    sc.stop()
  }
}
