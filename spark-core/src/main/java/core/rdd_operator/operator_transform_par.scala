package core.rdd_operator

import org.apache.spark.{SparkConf, SparkContext}

object operator_transform_par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    //map
    val rdd = sc.makeRDD(List(1, 2, 3, 4),numSlices = 3) //三个分区1；2 3；4
    //同一分区计算是有序的，不同分区之间计算是无序的
    val mapRDD = rdd.map(
      num => {
        println(">>>>>" + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("#####" + num)
        num
      }
    )

    //mapRDD1.collect()
    mapRDD1.collect()

    sc.stop()
  }

}
