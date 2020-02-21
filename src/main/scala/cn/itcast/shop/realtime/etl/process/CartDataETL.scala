package cn.itcast.shop.realtime.etl.process

import cn.itcast.shop.realtime.etl.bean.{CartBean, CartWideBean, DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity}
import cn.itcast.shop.realtime.etl.process.base.MQBaseETL
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import cn.itcast.util.ip.IPSeeker
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

/**
 * 购物车ETL操作类
 * @param env
 */
class CartDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    /**
     * 实现思路：
     * 1：直接读取购物车的topic数据
     * 2：将购物车的数据字符串转换成bean对象，同时对数据进行拉宽操作
     * 3：将拉宽厚的bean对象序列化成json字符串
     * 4：将序列化后的购物车json字符串写入到kafka的集群中
     */
    val cartDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.cart`)

    //2：将购物车的数据字符串转换成bean对象，同时对数据进行拉宽操作
    val cartBeanDataStream: DataStream[CartBean] = cartDataStream.map(cart=>{
      CartBean(cart)
    })

    val cartWideBeanDS = cartBeanDataStream.map(new RichMapFunction[CartBean, CartWideBean] {
      var jedis: Jedis = _
      var ipSeeker: IPSeeker = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
        ipSeeker = new IPSeeker(getRuntimeContext.getDistributedCache.getFile("qqwry.dat"))
      }

      override def close(): Unit = {
        if (jedis != null && jedis.isConnected) {
          jedis.close()
        }
      }

      override def map(cartBean: CartBean): CartWideBean = {
        val cartWideBean = CartWideBean(cartBean)

        try {
          // 拉宽商品
          val goodsJSON = jedis.hget("itcast_shop:dim_goods", cartWideBean.goodsId).toString
          val dimGoods = DimGoodsDBEntity(goodsJSON)
          // 获取商品三级分类数据
          val goodsCat3JSON = jedis.hget("itcast_shop:dim_goods_cats", dimGoods.getGoodsCatId.toString).toString
          val dimGoodsCat3 = DimGoodsCatDBEntity(goodsCat3JSON)
          // 获取商品二级分类数据
          val goodsCat2JSON = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsCat3.parentId).toString
          val dimGoodsCat2 = DimGoodsCatDBEntity(goodsCat2JSON)
          // 获取商品一级分类数据
          val goodsCat1JSON = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsCat2.parentId).toString
          val dimGoodsCat1 = DimGoodsCatDBEntity(goodsCat1JSON)

          // 获取商品店铺数据
          val shopJSON = jedis.hget("itcast_shop:dim_shops", dimGoods.shopId.toString).toString
          val dimShop = DimShopsDBEntity(shopJSON)

          // 获取店铺管理所属城市数据
          val orgCityJSON = jedis.hget("itcast_shop:dim_org", dimShop.areaId.toString).toString
          val dimOrgCity = DimOrgDBEntity(orgCityJSON)

          // 获取店铺管理所属省份数据
          val orgProvinceJSON = jedis.hget("itcast_shop:dim_org", dimOrgCity.parentId.toString).toString
          val dimOrgProvince = DimOrgDBEntity(orgProvinceJSON)

          // 设置商品数据
          cartWideBean.goodsPrice = dimGoods.shopPrice
          cartWideBean.goodsName = dimGoods.goodsName
          cartWideBean.goodsCat3 = dimGoodsCat3.catName
          cartWideBean.goodsCat2 = dimGoodsCat2.catName
          cartWideBean.goodsCat1 = dimGoodsCat1.catName
          cartWideBean.shopId = dimShop.shopId.toString
          cartWideBean.shopName = dimShop.shopName
          cartWideBean.shopProvinceId = dimOrgProvince.orgId.toString
          cartWideBean.shopProvinceName = dimOrgProvince.orgName
          cartWideBean.shopCityId = dimOrgCity.orgId.toString
          cartWideBean.shopCityName = dimOrgCity.orgName

          //解析IP数据
          val country = ipSeeker.getCountry(cartWideBean.ip)
          var areaArray = country.split("省");
          if (areaArray.length > 1) {
            cartWideBean.clientProvince = areaArray(0) + "省";
            cartWideBean.clientCity = areaArray(1)
          }
          else {
            areaArray = country.split("市");
            if (areaArray.length > 1) {
              cartWideBean.clientProvince = areaArray(0) + "市";
              cartWideBean.clientCity = areaArray(1)
            }
            else {
              cartWideBean.clientProvince = areaArray(0);
              cartWideBean.clientCity = ""
            }
          }

          // TODO: 拉宽时间数据, 这里会抛出异常，因为DateFormatUtils对象是非线程安全的
          // cartWideBean.year = DateFormatUtils.format(cartWideBean.addTime.toLong, "yyyy")
          // cartWideBean.month = DateFormatUtils.format(cartWideBean.addTime.toLong, "MM")
          // cartWideBean.day = DateFormatUtils.format(cartWideBean.addTime.toLong, "dd")
          // cartWideBean.hour = DateFormatUtils.format(cartWideBean.addTime.toLong, "HH")
        } catch {
          case ex => println(ex)
        }
        cartWideBean
      }
    })

    // 4：将cartWideBeanDS转换成json字符串返回，因为kafka中需要传入字符串类型的数据
    val cartWideJsonDataStream: DataStream[String] = cartWideBeanDS.map(cartWideEntityEntity => {
      JSON.toJSONString(cartWideEntityEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    cartWideJsonDataStream.printToErr("拉宽后的购物车明细对象>>>")

    //5：将关联维度表后的数据写入到kafka中，供Druid进行指标分析
    cartWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.cart`))
  }
}