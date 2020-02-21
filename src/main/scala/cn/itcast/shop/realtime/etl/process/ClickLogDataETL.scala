package cn.itcast.shop.realtime.etl.process

import java.io.File

import cn.itcast.shop.realtime.etl.bean.{ClickLogBean, ClickLogWideBean}
import cn.itcast.shop.realtime.etl.process.base.MQBaseETL
import cn.itcast.shop.realtime.etl.utils.DateUtil.{date2DateStr, datetime2date}
import cn.itcast.shop.realtime.etl.utils.GlobalConfigUtil
import cn.itcast.util.ip.IPSeeker
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import nl.basjes.parse.httpdlog.HttpdLoglineParser
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * 点击流日志的实时ETL操作
 * 需要将点击流日志对象转换成拓宽后点击流日志对象，增加省份、城市、时间字段
 * @param env
 */
case class ClickLogDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {

  /**
   * 处理数据的接口
   */
  override def process(): Unit = {
    /**
     * 实现思路：
     * 1：获取到点击流日志的数据，从kafka中
     * 2：将点击流bean对象转换成拓宽后的点击流对象
     * 3：要将拓宽后的点击流对象转换成json字符串
     * 4：将转后的json字符串写入到kafka的集群中
     */
    //1：获取到点击流日志的数据，从kafka中
    val clickLogDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.click_log`)

    //2：将点击流bean对象转换成拓宽后的点击流对象
    val clickLogWideBean: DataStream[ClickLogWideBean] = etl(clickLogDataStream)

    //3：要将拓宽后的点击流对象转换成json字符串
    val clickLogWideJsonDataStream: DataStream[String] = clickLogWideBean.map(logBean => {
      JSON.toJSONString(logBean, SerializerFeature.DisableCircularReferenceDetect)
    })

    clickLogWideJsonDataStream.printToErr("拉宽后的点击流对象>>>")

    //4：将转后的json字符串写入到kafka的集群中
    clickLogWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.clicklog`))
  }

  /**
   * 将点击流日志转换成拓宽后的点击流对象
   *
   * @param clickLogDataStream
   */
  def etl(clickLogDataStream: DataStream[String]) = {
    /** *
     * 实现思路：
     * 1：将点击流日志字符串转换成点击流对象（使用httpdlog）
     * 2：根据ip地址，获取到ip地址所对应的省份和城市信息，这里需要一个ip地址库，传递ip地址获取ip地址所对应的省份和城市
     * 3：将获取到的省份和城市作为拉宽后对象的参数传递进去，然后将拉宽后的对象返回
     */
    //1：将点击流日志字符串转换成点击流对象（使用httpdlog）
    val clickLogBeanDataStream: DataStream[ClickLogBean] = clickLogDataStream.map(new RichMapFunction[String, ClickLogBean] {
      //定义数据格式化的对象
      var parser: HttpdLoglineParser[ClickLogBean] = _

      //只被初始化一次
      override def open(parameters: Configuration): Unit = {
        //获取到序列化对象的映射规则
        parser = ClickLogBean.createClickLogParse()
      }

      //对数据进行一条条的处理
      override def map(value: String): ClickLogBean = {
        //传递点击流字符串返回点击流对象
        ClickLogBean(parser, value)
      }
    })

    //打印测试
    //clickLogBeanDataStream.printToErr()
    //2：根据ip地址，获取到ip地址所对应的省份和城市信息，这里需要一个ip地址库，传递ip地址获取ip地址所对应的省份和城市
    //将一种数据类型（ClickLogBean），转换成另外一种数据（ClickLogWideBean）
    clickLogBeanDataStream.map(new RichMapFunction[ClickLogBean, ClickLogWideBean] {
      //定义ip获取对象的实例
      var ipSeeker: IPSeeker = _

      //这个方法只被执行一次，可以在这个方法获取到分布式缓存数据
      override def open(parameters: Configuration): Unit = {
        val dataFile: File = getRuntimeContext.getDistributedCache.getFile("qqwry.dat")
        //初始化ipSeeker对象
        ipSeeker = new IPSeeker(dataFile)
      }

      //对传入的数据进行一条条的转换
      override def map(in: ClickLogBean): ClickLogWideBean = {
        //拷贝点击流日志对象
        val clickLogWideBean: ClickLogWideBean = ClickLogWideBean(in)

        //根据ip地址获取省份城市信息
        val country: String = ipSeeker.getCountry(clickLogWideBean.ip)
        var areaArray: Array[String] = country.split("省")

        if (areaArray.length > 1) {
          //不是直辖市
          clickLogWideBean.province = areaArray(0) + "省"
          clickLogWideBean.city = areaArray(1)
        } else {
          //直辖市
          areaArray = country.split("市")
          if (areaArray.length > 1) {
            clickLogWideBean.province = areaArray(0) + "市"
            clickLogWideBean.city = areaArray(1)
          } else {
            clickLogWideBean.province = areaArray(0)
            clickLogWideBean.city = ""
          }
        }
        //将访问时间转换成指定的字符串
        clickLogWideBean.requestDateTime = date2DateStr(datetime2date(clickLogWideBean.requestTime), "yyyy-MM-dd HH:mm:ss")
        clickLogWideBean
      }
    })
  }

}
