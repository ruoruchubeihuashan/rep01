package cn.itcast.shop.realtime.etl.process

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.itcast.shop.realtime.etl.process.base.MysqlBaseETL
import cn.itcast.shop.realtime.etl.utils.RedisUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis

/**
 * 维度数据的实时同步，同步到redis中
 * 数据来自于mysql->canal->kafka->kafka
 * @param env
 */
case class SyncDimDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    //1：获取到binlog日志数据
    val canalDataStream: DataStream[RowData] = getKafkaDataStream()

    //canal同步过来的数据是包含了itcast_shop库中的所有的表，我们只需要关心维度表的数据
    //2：过滤出来维度表的数据
    //filter必须需要一个返回值，如果条件满足则返回true，否则需要返回false，而之前没有加_，表示没有else，所以没有返回值，报错！
    //因此报错以后，我们设置了重启策略，所以不停的尝试重启执行
    val dimRowDataStream: DataStream[RowData] = canalDataStream.filter {
      rowData =>
        rowData.getTableName match {
          case "itcast_goods" => true
          case "itcast_goods_cats" => true
          case "itcast_shops" => true
          case "itcast_org" => true
          case "itcast_shop_cats" => true
          case _ => false
        }
    }

    dimRowDataStream.printToErr()

    //3：处理同步过来的数据，更新redis
    //使用函数还是使用对象，正确答案：对象
    dimRowDataStream.addSink(sinkFunction = new RichSinkFunction[RowData] {
      //定义redis的对象
      var redis: Jedis = _

      //这个方法只被执行一次，初始化资源的时候调用
      override def open(parameters: Configuration): Unit = {
        //实例化这个对象
        redis = RedisUtil.getJedis()
        //指定数据库
        redis.select(1)
      }

      //这个方法也是只被执行一次，关闭资源的时候调用
      override def close(): Unit = {
        //如果这个链接是链接状态中
        if(redis != null && redis.isConnected){
          redis.close()
        }
      }

      //处理数据，对dimRowDataStream数据一条条处理
      override def invoke(rowData: RowData, context: SinkFunction.Context[_]): Unit = {
        //eventType：insert/update/delete
        rowData.getEventType match {
          case eventType if(eventType == "insert" || eventType == "update") => updateDimData(rowData)
          case "delete" => deleteDimData(rowData)
          case _ =>
        }
      }

      //更新数据
      private def  updateDimData(rowData: RowData): Unit ={
        //需要区分那个维度表
        rowData.getTableName match{
          case "itcast_goods" =>{
            //更新商品维度表的数据
            val goodsid = rowData.getColumns.get("goodsId").toLong
            val goodsName = rowData.getColumns.get("goodsName")
            val goodsCatId = rowData.getColumns.get("goodsCatId").toInt
            val shopPrice = rowData.getColumns.get("shopPrice").toDouble
            val shopId = rowData.getColumns.get("shopId").toLong

            val goodsEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsid, goodsName, shopId, goodsCatId, shopPrice)
            redis.hset("itcast_shop:dim_goods", goodsid.toString, JSON.toJSONString(goodsEntity, SerializerFeature.DisableCircularReferenceDetect))
          } case "itcast_shops" => {
            //更新店铺表的数据
            val shopId = rowData.getColumns.get("shopId")
            val areaId = rowData.getColumns.get("areaId")
            val shopName = rowData.getColumns.get("shopName")
            val shopCompany = rowData.getColumns.get("shopCompany")

            val dimShop = DimShopsDBEntity(shopId.toInt, areaId.toInt, shopName, shopCompany)
            println("增量更新：" + dimShop)
            redis.hset("itcast_shop:dim_shops", shopId + "", JSON.toJSONString(dimShop, SerializerFeature.DisableCircularReferenceDetect))
          }
          case "itcast_goods_cats" => {
            val catId = rowData.getColumns.get("catId")
            val parentId = rowData.getColumns.get("parentId")
            val catName = rowData.getColumns.get("catName")
            val cat_level = rowData.getColumns.get("cat_level")

            val entity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
            println("增量更新：" + entity)

            redis.hset("itcast_shop:dim_goods_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
          }
          case "itcast_org" => {
            val orgId = rowData.getColumns.get("orgId")
            val parentId = rowData.getColumns.get("parentId")
            val orgName = rowData.getColumns.get("orgName")
            val orgLevel = rowData.getColumns.get("orgLevel")

            val entity = DimOrgDBEntity(orgId.toInt, parentId.toInt, orgName, orgLevel.toInt)
            println("增量更新：" + entity)
            redis.hset("itcast_shop:dim_org", orgId + "", JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
          }
          case "itcast_shop_cats" =>{
            val catId = rowData.getColumns.get("catId")
            val parentId = rowData.getColumns.get("parentId")
            val catName = rowData.getColumns.get("catName")
            val cat_level = rowData.getColumns.get("catSort")

            val entity = DimShopCatDBEntity(catId, parentId, catName, cat_level)
            println("增量更新：" + entity)
            redis.hset("itcast_shop:dim_shop_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
          }
        }
      }

      //删除数据
      private def deleteDimData(rowData: RowData) ={
        rowData.getTableName match {
          case "itcast_goods" => {
            redis.hdel("itcast_shop:dim_goods", rowData.getColumns.get("goodsId"))
          }
          case "itcast_shops" => {
            redis.hdel("itcast_shop:dim_shops", rowData.getColumns.get("shopId"))
          }
          case "itcast_goods_cats" => {
            redis.hdel("itcast_shop:dim_goods_cats", rowData.getColumns.get("catId"))
          }
          case "itcast_org" => {
            redis.hdel("itcast_shop:dim_org", rowData.getColumns.get("orgId"))
          }
          case "itcast_shop_cats" => {
            redis.hdel("itcast_shop:dim_shop_cats", rowData.getColumns.get("catId"))
          }
        }
      }
    })
  }
}
