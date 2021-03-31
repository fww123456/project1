package core.RDDBuild

import org.apache.spark.{SparkConf, SparkContext}

object Neicun_Create {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4)  //数据
    //val rdd = sc.parallelize(seq)  //创建rdd，parallelize并行
    val rdd = sc.makeRDD(seq) //简化的parallelize方法

    rdd.collect().foreach(println)

    //关闭环境
    sc.stop()
  }

}
