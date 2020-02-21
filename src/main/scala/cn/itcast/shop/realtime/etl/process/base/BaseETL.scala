package cn.itcast.shop.realtime.etl.process.base

import cn.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/**
 * 封装公共的ETL接口
 */
trait BaseETL[T] {

  /**
   * 构建kafka的生产者对象
   * @param topic
   */
  def  kafkaProducer(topic:String)={
    //将所有的etl后的数据写入到kafka中，写入的数据类型都是Json-》String
    new FlinkKafkaProducer011[String](
      topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),//对key也进行序列化
      KafkaProps.getKafkaProperties()
    )
  }

  /**
   * 从kafka中读取数据，传递返回的数据类型
   * @param topic
   * @return
   */
  def getKafkaDataStream(topic:String):DataStream[T];

  /**
   * 处理数据的接口
   */
  def process():Unit;
}
