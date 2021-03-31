package core.distribution

//Task对象不能直接传输，需要Serializable序列化
class Task extends Serializable {
  val datas = List(1,2,3,4)

  val logic = (num:Int)=>(num*2) //匿名函数，左边是一个参数，右边是函数体

  //计算，logic用在datas上
  def compute()={
    datas.map(logic)
  }
}
