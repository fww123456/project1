package core.RDDBuild

import org.apache.spark.{SparkConf, SparkContext}

object Neicun_Create_par {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //创建RDD
    //RDD的并行度&分区
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4), numSlices = 2
    )//分区数量为2，numSlices默认值为当前运行环境的最大可用核数

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")  //结果生成一个output文件，里面有两个part，分别装有1，2；3，4

    //关闭环境
    sc.stop()
  }

}
