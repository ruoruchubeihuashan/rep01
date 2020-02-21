package cn.itcast.shop.realtime.etl.process

import cn.itcast.shop.realtime.etl.bean.OrderDBEntity
import cn.itcast.shop.realtime.etl.process.base.MysqlBaseETL
import cn.itcast.shop.realtime.etl.utils.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 订单数据的ETL处理
 *
 * 订单数据写入到Druid的时候，需要注意：
 * 如果需要根据订单状态进行分析数据，需要将订单的id写进去，作为维度字段，否则聚合后的数据，
 * 无法区分哪些订单经过了哪些状态更新
 * @param env
 */
class OrderDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    /**
     * 实现思路：
     * 1：从kafka中消费订单数据（canal同步的数据是itcast_shop数据库中所有的表，但是我们现在处理的是order）
     * 2：将rowData对象转换成OrderDbEntity对象
     * 3：将OrderDBEntity对象转换成json字符串
     * 4：将转换好的json字符串写入到kafka中，供Druid实时摄取
     */
    //1：从kafka中消费订单数据（canal同步的数据是itcast_shop数据库中所有的表，但是我们现在处理的是order）
    val orderDataStream = getKafkaDataStream()
      .filter(_.getTableName == "itcast_orders")
      .filter(_.getEventType == "insert")

    orderDataStream.printToErr("订单对象>>>")
    /**
     * 需要注意：
     * 1：实时数据：如果写入kafka的订单数据->Druid摄取
     *    因为每次订单数据的更新都会同步到canal中，所以为了避免订单的重复计算，所以需要对订单数据进行去重写入kafka
     * 2：对于将订单数据写入hbase而言，一定需要考虑订单的更新问题，不需要考虑去重问题
     */

    //2：将rowData对象转换成OrderDbEntity对象
    val orderDBEntityDataStream: DataStream[OrderDBEntity] = orderDataStream.map(rowData=>{
      OrderDBEntity(rowData)
    })

    // 3：将OrderDBEntity对象转换成json字符串
    val orderDBEntityJsonDataStream: DataStream[String] = orderDBEntityDataStream.map(orderEntity => {
      JSON.toJSONString(orderEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    orderDBEntityJsonDataStream.printToErr("拉宽后的订单对象>>>")

    //4：将转换好的json字符串写入到kafka中，供Druid实时摄取
    orderDBEntityJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order`))
  }

}
