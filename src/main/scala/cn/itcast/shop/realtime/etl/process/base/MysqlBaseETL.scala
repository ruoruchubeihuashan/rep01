package cn.itcast.shop.realtime.etl.process.base

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.utils.{CanalRowDataDeserizationSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

/**
 * 编写mysql数据处理的基类，该类中处理的数据是RowData类型的数据
 */
abstract class MysqlBaseETL(env:StreamExecutionEnvironment) extends BaseETL[RowData] {
  /**
   * 从kafka中读取数据，传递返回的数据类型
   * 1：不管是读取的是订单数据、订单明细数据、商品数据，这些数据都是来自一个相同的topic：ods_itcast_shop_mysql
   * 2：对于以上三类数据，反序列化方式也是一样的，因为存储的都是protobuf数据
   * @param topic
   * @return
   */
  override def getKafkaDataStream(topic: String = "ods_itcast_shop_mysql"): DataStream[RowData] = {
    //现在消费的是kafka中的binlog数据，而在canalclient写入到kafka的数据是：RowData
    val canalKafkaConsumer: FlinkKafkaConsumer011[RowData] = new FlinkKafkaConsumer011[RowData](
      topic,
      //new SimpleStringSchema()，不可以这样写，因为现在kafka存储的是RowData对象，
      //而这个对象是我们自己定义的，所以说我们需要自己写一个反序列化类
      new CanalRowDataDeserizationSchema(),
      //kafka的properties对象
      KafkaProps.getKafkaProperties()
    )

    //将消费者添加到env环境中
    val canalRowDataDS: DataStream[RowData] = env.addSource(canalKafkaConsumer)
    //将获取到的数据返回
    canalRowDataDS
  }

//  /**
//   * 处理数据的接口，这个接口不可以在这里实现，因为商品数据、订单数据、订单明细数据有各自的处理逻辑
//   */
//  override def process(): Unit = ???
}
