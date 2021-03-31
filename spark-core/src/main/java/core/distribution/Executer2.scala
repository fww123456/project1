package core.distribution

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executer2 {
  def main(args: Array[String]): Unit = {

    val server = new ServerSocket(8888)  //启动服务器，接收数据
    println("服务器启动，等待接收数据")

    val client = server.accept()  //等待客户端的连接，只要客户端连接过来，就会得到一个连接对象

    val in = client.getInputStream  //输入流
    val objIn = new ObjectInputStream(in) //对象输入流

    val task = objIn.readObject().asInstanceOf[SubTask]  //读对象，asInstanceOf用来转化数据类型
    val ints = task.compute()
    println("计算结点8888计算的结果为：" + ints)

    objIn.close()
    client.close()
    server.close()
  }

}
