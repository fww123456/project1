package core.RDDBuild

import org.apache.spark.{SparkConf, SparkContext}

object file_Create {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //path路径为当前环境的根路径为基准。可以是绝对或者相对
    //val rdd = sc.textFile("datas/1.txt")
    //path路径可以是文件的具体路径，也可以是目录名
    //val rdd = sc.textFile("datas")
    //path路径还可以使用通配符*
    val rdd = sc.textFile("datas/1*.txt")
    //path还可以是分布式存储系统路径:hdfs://linux1:8020/test.txt



    rdd.collect().foreach(println)

    //关闭环境
    sc.stop()
  }

}
