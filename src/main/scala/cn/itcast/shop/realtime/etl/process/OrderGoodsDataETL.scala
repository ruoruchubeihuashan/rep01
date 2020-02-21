package cn.itcast.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.async.AsyncOrderDetailRedisRequst
import cn.itcast.shop.realtime.etl.bean.OrderGoodsWideEntity
import cn.itcast.shop.realtime.etl.process.base.MysqlBaseETL
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Delete, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * 订单明细表的实时ETL处理
 * 1：将订单明细的事实表数据与维度表数据进行拉宽后写入到hbase中
 * 2：将拉宽后的数据写入到kafka中，供Druid实时摄取
 *
 * 注意：
 * 1:写入hbase的数据不需要考虑数据重复问题，因为订单明细修改了以后hbase也要同步修改，不管操作是inser/update/delete,都需要操作hbase
 * 2:写到kafka的数据需要考虑重复问题，因为订单明细数据不需要重复累加，只需要操作insert即可
 * @param env
 */
class OrderGoodsDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    /**
     * 实现思路：
     * 1：获取canal中的订单明细数据，过滤出来数据，将rowData转换成订单明细对象
     * 2：将订单明细数据进行实时的拉宽操作（与维度表进行关联）
     * 3：将拉宽后的订单明细数据转换成json字符串写入到kafka中
     * 4：将拉宽后的订单明细数据写入到hbase中进行备份，以供后期数据的查询
     */
    val  orderGoodsCanalDataStream: DataStream[RowData] = getKafkaDataStream().filter(_.getTableName=="itcast_order_goods")

    //这里暂时不对订单明细表的insert操作过滤，因为订单明细表的更新需要反应到hbase中
    //2：将订单明细数据进行实时的拉宽操作（与维度表进行关联）
    //orderGoodsCanalDataStream.map()：将一种数据类型转换成另外一种数据类型的时候用Map，但是这种方式在这里不是最佳方案, map映射的时候是一种同步操作
    /**
     * 参数1：要关联的数据流
     * 参数2：超时时间
     * 参数3：时间单位
     * 参数4：异步IO最大的并发数
     */
    val orderGoodsWideDataStream: DataStream[OrderGoodsWideEntity] = AsyncDataStream.unorderedWait(orderGoodsCanalDataStream, new AsyncOrderDetailRedisRequst(), 1, TimeUnit.SECONDS, 100)

//    orderGoodsWideDataStream.printToErr()
    //3：将拉宽后的订单明细数据转换成json字符串写入到kafka中
    val orderGoodsWideJsonDataStream: DataStream[String] = orderGoodsWideDataStream.filter(_.eventType == "insert").map(orderGoodsBean => {
      JSON.toJSONString(orderGoodsBean, SerializerFeature.DisableCircularReferenceDetect)
    })

    orderGoodsWideJsonDataStream.printToErr("拉宽后的订单明细对象>>>")

    //供Druid进行实时的摄取，摄取的数据都是订单明细创建的数据
    orderGoodsWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order_detail`))

    //4：将拉宽后的订单明细数据写入到hbase中进行备份，以供后期数据的查询（不管对订单明细什么操作，都需要反应到hbase）
    orderGoodsWideDataStream.addSink(new RichSinkFunction[OrderGoodsWideEntity] {
      //定于hbase的连接对象
      var connection:Connection = _
      var table:Table =
        _
      //初始化资源，打开数据库的连接
      override def open(parameters: Configuration): Unit = {
        //实例化连接对象
        connection = HbaseUtil.getPool().getConnection
        table = connection.getTable(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))
      }

      //释放资源，关闭连接
      override def close(): Unit = {
        if(table != null) table.close()
        if(!connection.isClosed){
          //将连接放回到连接池中
          HbaseUtil.getPool().returnConnection(connection)
        }
      }

      //对数据一条条的处理
      override def invoke(orderGoodsWideEntity: OrderGoodsWideEntity, context: SinkFunction.Context[_]): Unit = {
        //构建Put对象
        val rowKey = Bytes.toBytes(orderGoodsWideEntity.ogId.toString)
        val put: Put = new Put(rowKey)
        val family = Bytes.toBytes(GlobalConfigUtil.`hbase.table.family`)

        val ogIdCol = Bytes.toBytes("ogId")
        val orderIdCol = Bytes.toBytes("orderId")
        val goodsIdCol = Bytes.toBytes("goodsId")
        val goodsNumCol = Bytes.toBytes("goodsNum")
        val goodsPriceCol = Bytes.toBytes("goodsPrice")
        val goodsNameCol = Bytes.toBytes("goodsName")
        val shopIdCol = Bytes.toBytes("shopId")
        val goodsThirdCatIdCol = Bytes.toBytes("goodsThirdCatId")
        val goodsThirdCatNameCol = Bytes.toBytes("goodsThirdCatName")
        val goodsSecondCatIdCol = Bytes.toBytes("goodsSecondCatId")
        val goodsSecondCatNameCol = Bytes.toBytes("goodsSecondCatName")
        val goodsFirstCatIdCol = Bytes.toBytes("goodsFirstCatId")
        val goodsFirstCatNameCol = Bytes.toBytes("goodsFirstCatName")
        val areaIdCol = Bytes.toBytes("areaId")
        val shopNameCol = Bytes.toBytes("shopName")
        val shopCompanyCol = Bytes.toBytes("shopCompany")
        val cityIdCol = Bytes.toBytes("cityId")
        val cityNameCol = Bytes.toBytes("cityName")
        val regionIdCol = Bytes.toBytes("regionId")
        val regionNameCol = Bytes.toBytes("regionName")

        put.addColumn(family, ogIdCol, Bytes.toBytes(orderGoodsWideEntity.ogId.toString))
        put.addColumn(family, orderIdCol, Bytes.toBytes(orderGoodsWideEntity.orderId.toString))
        put.addColumn(family, goodsIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsId.toString))
        put.addColumn(family, goodsNumCol, Bytes.toBytes(orderGoodsWideEntity.goodsNum.toString))
        put.addColumn(family, goodsPriceCol, Bytes.toBytes(orderGoodsWideEntity.goodsPrice.toString))
        put.addColumn(family, goodsNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsName.toString))
        put.addColumn(family, shopIdCol, Bytes.toBytes(orderGoodsWideEntity.shopId.toString))
        put.addColumn(family, goodsThirdCatIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatId.toString))
        put.addColumn(family, goodsThirdCatNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatName.toString))
        put.addColumn(family, goodsSecondCatIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatId.toString))
        put.addColumn(family, goodsSecondCatNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatName.toString))
        put.addColumn(family, goodsFirstCatIdCol, Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatId.toString))
        put.addColumn(family, goodsFirstCatNameCol, Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatName.toString))
        put.addColumn(family, areaIdCol, Bytes.toBytes(orderGoodsWideEntity.areaId.toString))
        put.addColumn(family, shopNameCol, Bytes.toBytes(orderGoodsWideEntity.shopName.toString))
        put.addColumn(family, shopCompanyCol, Bytes.toBytes(orderGoodsWideEntity.shopCompany.toString))
        put.addColumn(family, cityIdCol, Bytes.toBytes(orderGoodsWideEntity.cityId.toString))
        put.addColumn(family, cityNameCol, Bytes.toBytes(orderGoodsWideEntity.cityName.toString))
        put.addColumn(family, regionIdCol, Bytes.toBytes(orderGoodsWideEntity.regionId.toString))
        put.addColumn(family, regionNameCol, Bytes.toBytes(orderGoodsWideEntity.regionName.toString))

        //执行put操作
        if(orderGoodsWideEntity.eventType == "delete"){
          //如果这个订单明细被删除了，这个订单明细钟的商品也被删除
          val delete = new Delete(rowKey)
          table.delete(delete)
        }else {
          //插入或者更新操作
          table.put(put)
        }
      }
    })
  }
}
