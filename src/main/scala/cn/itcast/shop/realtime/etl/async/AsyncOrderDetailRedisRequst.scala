package cn.itcast.shop.realtime.etl.async

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity, OrderGoodsWideEntity}
import cn.itcast.shop.realtime.etl.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * 异步查询订单明细数据与维度数据关联
 *
 * 根据事实表（订单明细表）与维度表（redis）进行关联，所以需要操作Redis，应该需要打开数据连接，关闭数据连接
 */
class AsyncOrderDetailRedisRequst extends RichAsyncFunction[RowData, OrderGoodsWideEntity]{
  //定义redis的对象
  var jedis:Jedis = _

  /**
   * 初始化资源，本方法只被执行一次
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    jedis = RedisUtil.getJedis()
    //维度数据在第二个数据库中
    jedis.select(1)
  }

  /**
   * 释放资源，关闭连接
   */
  override def close(): Unit = {
    if(jedis!= null && jedis.isConnected){
      jedis.close()
    }
  }

  /**
   * 连接redis超时
   * @param input
   * @param resultFuture
   */
  override def timeout(input: RowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    /**
     * 可以在这里面打印一些日志，比如说监控，监控redis获取维度数据的时候是否超时
     */
    println("订单明细拉宽操作的时候关联维度数据超时，请关注！")
  }

  //定义Future回调的执行上下文对象
  implicit lazy val executor = ExecutionContext.fromExecutor(Executors.directExecutor())

  /**
   * mapFunction:Invoke
   * 异步操作，对数据流中的数据一条条的处理，但是这个操作是一个异步操作
   *
   * @param rowData
   * @param resultFuture
   */
  override def asyncInvoke(rowData: RowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    //发送异步请求。获取请求结果Future,调用Future代码
    Future {
      if (!jedis.isConnected) {
        jedis = RedisUtil.getJedis()
        //维度数据在第二个数据库中
        jedis.select(1)
      }

      //1：根据goodsid获取商品数据（商品名称、商品对应的分类信息、店铺ID）
      val goodsJson: String = jedis.hget("itcast_shop:dim_goods", rowData.getColumns.get("goodsId"))
      //将json字符串反序列化成对象
      val dimGoods: DimGoodsDBEntity = DimGoodsDBEntity(goodsJson)

      //2：根据商品表的店铺id获取店铺数据（店铺的名字、店铺所在的组织机构）
      val shopJson: String = jedis.hget("itcast_shop:dim_shops", dimGoods.shopId.toString)
      //将店铺的json字符串转换成店铺的对象
      val dimShop: DimShopsDBEntity = DimShopsDBEntity(shopJson)

      //3：根据商品id获取商品对应的分类数据
      //3.1：获取商品的三级分类信息
      val thirdCatJson = jedis.hget("itcast_shop:dim_goods_cats", dimGoods.goodsCatId.toString)
      val dimThirdCat: DimGoodsCatDBEntity = DimGoodsCatDBEntity(thirdCatJson)

      //3.2：获取商品的二级分类信息
      val secondCatJson = jedis.hget("itcast_shop:dim_goods_cats", dimThirdCat.parentId.toString)
      val dimSecondCat = DimGoodsCatDBEntity(secondCatJson)

      //3.3：获取商品的一级分类信息
      val firstCatJson = jedis.hget("itcast_shop:dim_goods_cats", dimSecondCat.parentId.toString)
      val dimFirstCat = DimGoodsCatDBEntity(firstCatJson)

      //4：获取根据店铺表的区域id组织机构信息
      //4.1：根据区域id获取城市数据
      val cityJson: String = jedis.hget("itcast_shop:dim_org", dimShop.areaId.toString)
      val dimOrgCity: DimOrgDBEntity = DimOrgDBEntity(cityJson)

      //4.2：根据城市的父id获取大区数据
      val regionJson: String = jedis.hget("itcast_shop:dim_org", dimOrgCity.parentId.toString)
      val dimOrgRegion: DimOrgDBEntity = DimOrgDBEntity(regionJson)

      //构建订单明细宽表，返回宽表对象
      val orderGoodsWideBean: OrderGoodsWideEntity = OrderGoodsWideEntity(
        rowData.getColumns.get("ogId").toLong,
        rowData.getColumns.get("orderId").toLong,
        rowData.getColumns.get("goodsId").toLong,
        rowData.getColumns.get("goodsNum").toLong,
        rowData.getColumns.get("goodsPrice").toDouble,
        dimGoods.goodsName,
        dimShop.shopId,
        dimThirdCat.catId.toInt,
        dimThirdCat.catName,
        dimSecondCat.catId.toInt,
        dimSecondCat.catName,
        dimFirstCat.catId.toInt,
        dimFirstCat.catName,
        dimShop.areaId,
        dimShop.shopName,
        dimShop.shopCompany,
        dimOrgCity.orgId,
        dimOrgCity.orgName,
        dimOrgRegion.orgId,
        dimOrgRegion.orgName,
        rowData.getEventType //操作类型
      )

      //println(s"订单明细异步IO拉取的数据：${orderGoodsWideBean}")
      //异步请求回调
      resultFuture.complete(Array(orderGoodsWideBean))
    }
  }
}
