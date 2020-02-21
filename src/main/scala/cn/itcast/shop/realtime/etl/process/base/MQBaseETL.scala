package cn.itcast.shop.realtime.etl.process.base

import cn.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

/**
 * 编写点击流日志、评论、购物车等等数据的ETL处理基类，需要继承自BaseETL
 */
abstract  class MQBaseETL(env:StreamExecutionEnvironment) extends BaseETL[String] {
  /**
   * 从kafka中读取数据，传递返回的数据类型
   *
   * @param topic
   * @return
   */
  override def getKafkaDataStream(topic: String): DataStream[String] = {
    //编写kafka消费者对象实例
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      KafkaProps.getKafkaProperties()
    )

    //将消费者对象实例添加到数据源
    val logDataStream: DataStream[String] = env.addSource(kafkaConsumer)
    logDataStream
  }
}
