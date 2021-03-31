package core.rdd_operator

import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object operator_transform2 {
  def main(args: Array[String]): Unit = {

    val iter = List(1,2,3,4).iterator //得到迭代器
    //while遍历
    while(iter.hasNext){
      println(iter.next())
    }
    //for遍历
    刚刚是错的
    for(i<-iter){
      println(i)
    }

  }

}
