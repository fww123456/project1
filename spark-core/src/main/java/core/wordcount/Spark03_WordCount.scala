package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines = sc.textFile("datas")

    val words:RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word,1)
    )

    //spark提供更多的框架，可以将分组和聚类使用一个方法实现
    //reduceByKey：相同的key的数据，可以对value进行reduce聚合
    //wordToOne.reduceByKey((x,y)=>{x+y})
    //wordToOne.reduceByKey((x,y)=>x+y)
    val wordToCount = wordToOne.reduceByKey(_+_)


    //5.将采集的结果打印
    val array = wordToCount.collect()
    array.foreach(println)


    //关闭连接
    sc.stop()

  }

}
