package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

  def main(args: Array[String]): Unit = {
    //建立和spark框架链接
    //JDBC:Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务操作
    //1.读取文件，获取一行一行的数据
    val lines = sc.textFile("datas")


    //2.将一行数据进行拆分，形成一个一个的单词(一行的单词之间用空格分隔)
    val words:RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word,1)
    )
    //map匹配
    //（word）转化-->(word,1)


    //3.数据根据单词分组，便于统计
    val wordGroup = wordToOne.groupBy(t => t._1)
    //给t(word,1)，用t._1（word）分组
    //wordGroup：（word,((word,1),(word,1),(word,1))）


    //4.对分组进行转换，如（hello,hello,hello）(world,world)-->(hello,3)(world,2)
    val wordToCount = wordGroup.map{
      case (word,list)=>{

        list.reduce(
          (t1, t2) => {
            (t1._1, t2._2 + t1._2)
          }
        )

      }
    }
    //word=word
    //list=((word,1),(word,1),(word,1))
    //t1=(word,1)  t2=(word,1)
    //t1._1=word   t1._2=1


    //5.将采集的结果打印
    val array = wordToCount.collect()
    array.foreach(println)


    //关闭连接
    sc.stop()

  }

}
