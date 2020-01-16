package one_wordcount

import org.apache.flink.api.scala._

/**
  * local model
  * DataSet helloword
  */

object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val input_path = "hello.txt"
    //从文件里读取数据
    val input_ds: DataSet[String] = environment.readTextFile(input_path)
    //分词之后对对单次groupby,然后sum聚合
    val word_count_ds: AggregateDataSet[(String, Int)] =
      input_ds
        //扁平化
        .flatMap(_.split(" "))
        //映射新数据类型，此处的1是数字1
        .map((_, 1))
        //分组，数字字段是索引位置
        .groupBy(0)
        //sum
        .sum(1)
    word_count_ds.print()

  }
}
