package core.RDDBuild

import org.apache.spark.{SparkConf, SparkContext}

object file2_Create {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从文件中创建RDD，将文件中的数据作为处理的数据源
    val rdd = sc.wholeTextFiles("datas")
    //textFile：以行为单位读取数据，读取的数据是字符串
    //wholeTextFile：以文件名为单位读取数据，读取的结果为元组

    rdd.collect().foreach(println)

    //关闭环境
    sc.stop()
  }

}
