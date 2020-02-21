package cn.itcast.shop.realtime.etl.dataloader

import java.sql.{Connection, DriverManager, Statement}

import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import redis.clients.jedis.Jedis

/**
 * 这个单例对象是将mysql中的维度表的数据做离线同步到redis中（全量维度同步）
 * 这里有五个维度表：
 * 商品维度表
 * 商品分类维度表-》电商平台的商品分类表
 * 店铺表
 * 组织机构表
 * 门店商品分类-》比如大家开了一家网店，网店中有很多商品，你自己对于这些商品进行的分类
 */
object DimensionDataLoader {

  def main(args: Array[String]): Unit = {
    //1：注册mysql的驱动
    Class.forName("com.mysql.jdbc.Driver")

    //2：创建连接
    val connection: Connection = DriverManager.getConnection(s"jdbc:mysql://${GlobalConfigUtil.`mysql.server.ip`}:${GlobalConfigUtil.`mysql.server.port`}/${GlobalConfigUtil.`mysql.server.database`}",
      GlobalConfigUtil.`mysql.server.username`,
      GlobalConfigUtil.`mysql.server.password`)

    //3：创建redis的连接
    val jedis = RedisUtil.getJedis()
    //将维度表的数据放到第二个数据库，redis的数据库默认是16个
    jedis.select(1)

    //加载商品维度表的数据到redis
    LoadDimGoods(connection, jedis)
    loadDimShops(connection, jedis)
    loadDimGoodsCats(connection, jedis)
    loadDimOrg(connection, jedis)
    LoadDimShopCats(connection, jedis)
    System.exit(0)
  }

  /**
   * 加载商品维度表的数据到redis
   * @param connection
   * @param jedis
   */
  def LoadDimGoods(connection: Connection, jedis: Jedis): Unit = {
    //定义sql语句
    val querySql: String =
      """
        |SELECT
        |	t1.`goodsId`,
        |	t1.`goodsName`,
        |	t1.`goodsCatId`,
        | t1.`shopPrice`,
        |	t1.`shopId`
        |FROM
        |	itcast_goods t1
      """.stripMargin

    //创建statement
    val statement: Statement = connection.createStatement()
    val resultSet = statement.executeQuery(querySql)

    //循环商品表数据
    while(resultSet.next()){
      val goodsid = resultSet.getLong("goodsId")
      val goodsName = resultSet.getString("goodsName")
      val goodsCatId = resultSet.getInt("goodsCatId")
      val shopPrice = resultSet.getDouble("shopPrice")
      val shopId = resultSet.getLong("shopId")

      //将拿到的数据写入到redis中
      //redis的数据是k/v键值对类型，而我们现在要写入的数据是一个多字段的列表，怎么写入
      //可以将所有的字段封装成json字符串作为redis的value字段写入，后面使用的时候，需要解析json字符串获取各个字段的值
      //key一定是唯一的，在redis，所以可以将商品id作为key
      val goodsEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsid, goodsName, shopId, goodsCatId, shopPrice)
      //将goodsEntity对象转换成json字符串作为redis的value值
      //Error:(76, 66) ambiguous reference to overloaded definition,
      //both method toJSONString in object JSON of type (x$1: Any, x$2: com.alibaba.fastjson.serializer.SerializerFeature*)String
      //and  method toJSONString in object JSON of type (x$1: Any)String
      //match argument types (cn.itcast.shop.realtime.etl.bean.DimGoodsDBEntity)
      //      jedis.hset("itcast_shop:dim_goods", goodsid.toString, JSON.toJSONString(goodsEntity))
      //表示需要关闭循环引用
      jedis.hset("itcast_shop:dim_goods", goodsid.toString, JSON.toJSONString(goodsEntity, SerializerFeature.DisableCircularReferenceDetect))
    }
    resultSet.close()
    statement.close()
  }
  // 加载商铺维度数据到Redis
  // 加载商铺维度数据到Redis
  def loadDimShops(connection: Connection, jedis: Jedis) = {
    val sql =
      """
        |SELECT
        |	t1.`shopId`,
        |	t1.`areaId`,
        |	t1.`shopName`,
        |	t1.`shopCompany`
        |FROM
        |	itcast_shops t1
      """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val shopId = resultSet.getInt("shopId")
      val areaId = resultSet.getInt("areaId")
      val shopName = resultSet.getString("shopName")
      val shopCompany = resultSet.getString("shopCompany")

      val dimShop = DimShopsDBEntity(shopId, areaId, shopName, shopCompany)
      println(dimShop)
      jedis.hset("itcast_shop:dim_shops", shopId + "", JSON.toJSONString(dimShop, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }
  def loadDimGoodsCats(connection: Connection, jedis: Jedis) = {
    val sql = """
                |SELECT
                |	t1.`catId`,
                |	t1.`parentId`,
                |	t1.`catName`,
                |	t1.`cat_level`
                |FROM
                |	itcast_goods_cats t1
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("cat_level")

      val entity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
      println(entity)

      jedis.hset("itcast_shop:dim_goods_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }

  // 加载组织结构维度数据
  def loadDimOrg(connection: Connection, jedis: Jedis) = {
    val sql = """
                |SELECT
                |	orgid,
                |	parentid,
                |	orgName,
                |	orgLevel
                |FROM
                |	itcast_org
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val orgId = resultSet.getInt("orgId")
      val parentId = resultSet.getInt("parentId")
      val orgName = resultSet.getString("orgName")
      val orgLevel = resultSet.getInt("orgLevel")

      val entity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
      println(entity)
      jedis.hset("itcast_shop:dim_org", orgId + "", JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }
  }

  // 加载门店商品分类维度数据到Redis
  def LoadDimShopCats(connection: Connection, jedis: Jedis): Unit ={
    val sql = """
                |SELECT
                |	t1.`catId`,
                |	t1.`parentId`,
                |	t1.`catName`,
                |	t1.`catSort`
                |FROM
                |	itcast_shop_cats t1
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("catSort")

      val entity = DimShopCatDBEntity(catId, parentId, catName, cat_level)
      println(entity)

      jedis.hset("itcast_shop:dim_shop_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }

}
