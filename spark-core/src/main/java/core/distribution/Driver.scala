package core.distribution

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {

    val client = new Socket("localhost",9999)  //链接服务器
    val client2 = new Socket("localhost",8888)  //链接服务器

    //建立连接后，发送任务
    val out = client.getOutputStream  //输出流
    val out2 = client2.getOutputStream  //输出流
    val objOut = new ObjectOutputStream(out)  //对象输出流
    val objOut2 = new ObjectOutputStream(out2)  //对象输出流

    val task = new Task()  //创建对象
    val subTask = new SubTask()

    subTask.datas = task.datas.take(2)
    subTask.logic = task.logic
    objOut.writeObject(subTask)  //发送对象
    objOut.flush()
    objOut.close()
    client.close()
    println("客户端1数据发送完毕")

    val subTask1 = new SubTask()
    subTask1.datas = task.datas.takeRight(2)
    subTask1.logic = task.logic
    objOut2.writeObject(subTask1)  //发送对象
    objOut2.flush()
    objOut2.close()
    client2.close()
    println("客户端2数据发送完毕")
  }

}
