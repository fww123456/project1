package core.rdd_operator

import org.apache.spark.{SparkConf, SparkContext}

object operator_transform3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    //mapPartitions，把全部的数据拿到了之后再做操作
    val rdd = sc.makeRDD(List(1, 2, 3, 4), numSlices = 2)
    //【1，2】【3，4】

    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        //(分区编号，数字)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpiRDD.collect().foreach(println)

    sc.stop()
  }

}
/*
(0,1)
(0,2)
(1,3)
(1,4)
 */