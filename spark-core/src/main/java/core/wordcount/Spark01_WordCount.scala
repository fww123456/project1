package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    //建立和spark框架链接
    //JDBC:Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务操作
    //1.读取文件，获取一行一行的数据
    val lines = sc.textFile("datas")


    //2.将一行数据进行拆分，形成一个一个的单词(一行的单词之间用空格分隔)
    val words = lines.flatMap(_.split(" "))


    //3.数据根据单词分组，便于统计
    val wordGroup = words.groupBy(word => word) //给元素是word(前)，就按word(后)分组
    //wordGroup：（word,(word,word,word)）==（单词，集合）


    //4.对分组进行转换，如（hello,hello,hello）(world,world)-->(hello,3)(world,2)
    val wordToCount = wordGroup.map{
      case (word,list)=>{
        (word,list.size)
      }
    }
    //map用来匹配
    //（单词，集合）转化-->（单词，集合大小）



    //5.将采集的结果打印
    val array = wordToCount.collect()
    array.foreach(println) //foreach循环遍历


    //关闭连接
    sc.stop()

  }

}
