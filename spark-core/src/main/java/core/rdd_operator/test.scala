package core.rdd_operator

import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/word.txt")

    //挑选每行元素
    val mapRDD = rdd.map(
      line => {
        val data = line.split(" ") //以空格为界限隔开
        data(2)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
