package cn.itcast.shop.realtime.etl.process

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity, GoodsWideBean}
import cn.itcast.shop.realtime.etl.process.base.MysqlBaseETL
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

/**
 * 商品数据的ETL操作
 * @param env
 */
class GoodsDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    /**
     * 实现思路：
     * 1：找到kafka中保存的mysql数据，过滤出来商品表的数据
     * 2：将过滤出来的rowdata数据转换成GoodsWideBean实体对象，同时进行拉宽操作（关联维度表）
     * 3：将拉宽厚的商品表实体对象转换成json字符串
     * 4：将商品数据写入到kafka集群，供Druid实时摄取
     */
    //1：找到kafka中保存的mysql数据，过滤出来商品表的数据
    val goodsCanalDataStream: DataStream[RowData] = getKafkaDataStream().filter(_.getTableName == "itcast_goods")

    //2：将过滤出来的rowdata数据转换成GoodsWideBean实体对象，同时进行拉宽操作（关联维度表）
    //以前关联订单明细数据的时候使用的是异步io，我们这里最好使用异步io，但是为了促进学习，这里给大家演示使用同步io：MapFunction
    val goodsWideBeanDataStream: DataStream[GoodsWideBean] = goodsCanalDataStream.map(new RichMapFunction[RowData, GoodsWideBean] {
      //定义jedis对象
      var jedis: Jedis = _

      //初始化资源
      override def open(parameters: Configuration): Unit = {
        //获取redis的连接
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      //释放资源
      override def close(): Unit = {
        if (jedis != null && jedis.isConnected) {
          jedis.close()
        }
      }

      //对数据进行处理
      override def map(rowData: RowData): GoodsWideBean = {
        val shopJSON = jedis.hget("itcast_shop:dim_shops", rowData.getColumns.get("shopId") + "")
        val dimShop = DimShopsDBEntity(shopJSON)

        val thirdCatJSON = jedis.hget("itcast_shop:dim_goods_cats", rowData.getColumns.get("goodsCatId") + "")
        val dimThirdCat = DimGoodsCatDBEntity(thirdCatJSON)

        val secondCatJSON = jedis.hget("itcast_shop:dim_goods_cats", dimThirdCat.parentId)
        val dimSecondCat = DimGoodsCatDBEntity(secondCatJSON)

        val firstCatJSON = jedis.hget("itcast_shop:dim_goods_cats", dimSecondCat.parentId)
        val dimFirstCat = DimGoodsCatDBEntity(firstCatJSON)

        val secondShopCatJson = jedis.hget("itcast_shop:dim_shop_cats", rowData.getColumns.get("shopCatId1"))
        val dimSecondShopCat = DimShopCatDBEntity(secondShopCatJson)

        val firstShopCatJson = jedis.hget("itcast_shop:dim_shop_cats", rowData.getColumns.get("shopCatId2"))
        val dimFirstShopCat = DimShopCatDBEntity(firstShopCatJson)

        val cityJSON = jedis.hget("itcast_shop:dim_org", dimShop.areaId + "")
        val dimOrgCity = DimOrgDBEntity(cityJSON)

        val regionJSON = jedis.hget("itcast_shop:dim_org", dimOrgCity.parentId + "")
        val dimOrgRegion = DimOrgDBEntity(regionJSON)

        GoodsWideBean(rowData.getColumns.get("goodsId").toLong,
          rowData.getColumns.get("goodsSn"),
          rowData.getColumns.get("productNo"),
          rowData.getColumns.get("goodsName"),
          rowData.getColumns.get("goodsImg"),
          rowData.getColumns.get("shopId"),
          dimShop.shopName,
          rowData.getColumns.get("goodsType"),
          rowData.getColumns.get("marketPrice"),
          rowData.getColumns.get("shopPrice"),
          rowData.getColumns.get("warnStock"),
          rowData.getColumns.get("goodsStock"),
          rowData.getColumns.get("goodsUnit"),
          rowData.getColumns.get("goodsTips"),
          rowData.getColumns.get("isSale"),
          rowData.getColumns.get("isBest"),
          rowData.getColumns.get("isHot"),
          rowData.getColumns.get("isNew"),
          rowData.getColumns.get("isRecom"),
          rowData.getColumns.get("goodsCatIdPath"),
          dimThirdCat.catId.toInt,
          dimThirdCat.catName,
          dimSecondCat.catId.toInt,
          dimSecondCat.catName,
          dimFirstCat.catId.toInt,
          dimFirstCat.catName,
          dimFirstShopCat.getCatId,
          dimFirstShopCat.catName,
          dimSecondShopCat.getCatId,
          dimSecondShopCat.catName,
          rowData.getColumns.get("brandId"),
          rowData.getColumns.get("goodsDesc"),
          rowData.getColumns.get("goodsStatus"),
          rowData.getColumns.get("saleNum"),
          rowData.getColumns.get("saleTime"),
          rowData.getColumns.get("visitNum"),
          rowData.getColumns.get("appraiseNum"),
          rowData.getColumns.get("isSpec"),
          rowData.getColumns.get("gallery"),
          rowData.getColumns.get("goodsSeoKeywords"),
          rowData.getColumns.get("illegalRemarks"),
          rowData.getColumns.get("dataFlag"),
          rowData.getColumns.get("createTime"),
          rowData.getColumns.get("isFreeShipping"),
          rowData.getColumns.get("goodsSerachKeywords"),
          rowData.getColumns.get("modifyTime"),
          dimOrgCity.orgId,
          dimOrgCity.orgName,
          dimOrgRegion.orgId,
          dimOrgRegion.orgName)
      }
    })

    goodsWideBeanDataStream.printToErr("拉宽后的商品明细对象>>>")
    //3：将拉宽厚的商品表实体对象转换成json字符串
    val goodsWideBeanJsonDataStream: DataStream[String] = goodsWideBeanDataStream.map(goodsWideBean=>{
      JSON.toJSONString(goodsWideBean, SerializerFeature.DisableCircularReferenceDetect)
    })

    //4：将商品数据写入到kafka集群，供Druid实时摄取
    goodsWideBeanJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.goods`))
  }
}
