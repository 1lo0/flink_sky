package one_wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * local model
  * DataStream  wordcount
  *
  * @author xq
  * @create 2020-01-15 17:20
  **/
object DataStreamWordCount {
  def main(args: Array[String]): Unit = {
    //外部获取参数
    val parameterTool = ParameterTool.fromArgs(args)
    val host:String = parameterTool.get("host")
    val port:Int = parameterTool.getInt("port")

    //创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接受socket文本流
    val inputDS: DataStream[String] = environment.socketTextStream(host, port)
    //flatmap和map需要隐式转换
    import org.apache.flink.api.scala._
    //扁平化，过滤空，映射新数据结构，按key聚合，求和
    val resultDS: DataStream[(String, Int)] =
      inputDS.flatMap(_.split(" "))
        .filter(_.nonEmpty)
        .map((_, 1))
        .keyBy(0)
        .sum(1)
    resultDS.print().setParallelism(1)

    environment.execute("datastream word count")


  }
}
