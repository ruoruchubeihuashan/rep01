package cn.itcast.shop.realtime.etl.process

import java.sql.{Date, Timestamp}

import cn.itcast.shop.realtime.etl.bean.{Comments, CommentsWideEntity, DimGoodsDBEntity}
import cn.itcast.shop.realtime.etl.process.base.MQBaseETL
import cn.itcast.shop.realtime.etl.utils.{DateUtil, GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis
import  org.apache.flink.streaming.api.scala._

/**
 * 评论数据的实时ETL操作
 * @param env
 */
class CommentsDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    // 1. 整合Kafka
    val commentsDS: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.comments`)

    // 2. Flink实时ETL
    // 将JSON转换为实体类
    val commentsBeanDS: DataStream[Comments] = commentsDS.map(
      commentsJson=>{
        Comments(commentsJson)
      }
    )

    //将评论信息表进行拉宽操作
    val commentsWideBeanDataStream: DataStream[CommentsWideEntity] = commentsBeanDS.map(new RichMapFunction[Comments, CommentsWideEntity] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      //释放资源
      override def close(): Unit = {
        if (jedis !=null && jedis.isConnected) {
          jedis.close()
        }
      }

      //对数据进行拉宽
      override def map(comments: Comments): CommentsWideEntity = {
        // 拉宽商品
        val goodsJSON = jedis.hget("itcast_shop:dim_goods", comments.goodsId)
        val dimGoods = DimGoodsDBEntity(goodsJSON)

        //将时间戳转换为时间类型
        val timestamp = new Timestamp(comments.timestamp)
        val date = new Date(timestamp.getTime)

        CommentsWideEntity(
          comments.userId,
          comments.userName,
          comments.orderGoodsId,
          comments.starScore,
          comments.comments,
          comments.assetsViedoJSON,
          DateUtil.date2DateStr(date, "yyyy-MM-dd HH:mm:ss"),
          comments.goodsId,
          dimGoods.goodsName,
          dimGoods.shopId
        )
      }
    })

    //3:将评论信息数据拉宽处理
    val commentsJsonDataStream: DataStream[String] = commentsWideBeanDataStream.map(commentsWideEntity => {
      JSON.toJSONString(commentsWideEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    commentsJsonDataStream.printToErr("拉宽后的评论明细对象>>>")

    //4：将关联维度表后的数据写入到kafka中，供Druid进行指标分析
    commentsJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.comments`))
  }
}