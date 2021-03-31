package core.RDDBuild

import org.apache.spark.{SparkConf, SparkContext}

object file3_Create {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //minPartitions：最小分区数量
    val rdd = sc.textFile("datas/1.txt",minPartitions = 2)

    rdd.saveAsTextFile("output") //输出的分区数可能会被设定的要大

    //关闭环境
    sc.stop()
  }
}