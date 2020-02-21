package cn.itcast.shop.realtime.etl.bean

import jdk.nashorn.internal.objects.annotations.Setter
import lombok.{Data, Getter}
import nl.basjes.parse.httpdlog.HttpdLoglineParser

import scala.beans.BeanProperty

/**
 * 定义点击流的类，将kafka存储的点击流日志转换成bean对象，方便后期使用
 * 一般在企业中，编写代码的时候，定义的全局变量多数都会加上_
 *
 * 样例类和普通类的区别：
 * 1：样例类默认实现了序列化
 * 2：样例类使用的时候不需要new语法
 * 3：样例类可以进行模式匹配
 */
class ClickLogBean {

  //用户id信息
  var _connectionClientUser: String = _

  def setConnectionClientUser(value: String): Unit = {
    _connectionClientUser = value
  }

  def getConnectionClientUser = {
    _connectionClientUser
  }

  //ip地址
  private[this] var _ip: String = _

  def setIp(value: String): Unit = {
    _ip = value
  }

  def getIp = {
    _ip
  }

  //请求时间
  private[this] var _requestTime: String = _

  def setRequestTime(value: String): Unit = {
    _requestTime = value
  }

  def getRequestTime = {
    _requestTime
  }

  //请求方式
  private[this] var _method: String = _

  def setMethod(value: String) = {
    _method = value
  }

  def getMethod = {
    _method
  }

  //请求资源
  private[this] var _resolution: String = _

  def setResolution(value: String) = {
    _resolution = value
  }

  def getResolution = {
    _resolution
  }

  //请求协议
  private[this] var _requestProtocol: String = _

  def setRequestProtocol(value: String): Unit = {
    _requestProtocol = value
  }

  def getRequestProtocol = {
    _requestProtocol
  }

  //响应码
  private[this] var _responseStatus: Int = _

  def setRequestStatus(value: Int): Unit = {
    _responseStatus = value
  }

  def getRequestStatus = {
    _responseStatus
  }

  //返回的数据流量
  private[this] var _responseBodyBytes: String = _

  def setResponseBodyBytes(value: String): Unit = {
    _responseBodyBytes = value
  }

  def getResponseBodyBytes = {
    _responseBodyBytes
  }

  //访客的来源url
  private[this] var _referer: String = _

  def setReferer(value: String): Unit = {
    _referer = value
  }

  def getReferer = {
    _referer
  }

  //客户端代理信息
  private[this] var _userAgent: String = _

  def setUserAgent(value: String): Unit = {
    _userAgent = value
  }

  def getUserAgent = {
    _userAgent
  }

  //跳转过来页面的域名:HTTP.HOST:request.referer.host
  private[this] var _referDomain: String = _

  def setReferDomain(value: String): Unit = {
    _referDomain = value
  }

  def getReferDomain = {
    _referDomain
  }

  override def toString: String = {
    _connectionClientUser
  }
}

/**
 * 定义伴生对象
 */
object ClickLogBean{
  //1：定义点击流日志的解析规则
  val getLogFormat: String = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""

  //接收点击流日志字符串，返回点击流对象
  def apply(parser:HttpdLoglineParser[ClickLogBean], clickLog:String): ClickLogBean = {
    val clickLogBean = new ClickLogBean
    parser.parse(clickLogBean, clickLog)

    //将点击流对象返回
    clickLogBean
  }

  //定义点击流日志的映射规则，将读取到的属性赋值给bean对象
  def createClickLogParse() = {
    //创建日志解析器
    val parser = new HttpdLoglineParser[ClickLogBean](classOf[ClickLogBean], getLogFormat)

    //定义参数名的地址创建别名
    parser.addTypeRemapping("request.firstline.uri.query.g", "HTTP.URI")
    parser.addTypeRemapping("request.firstline.uri.query.r", "HTTP.URI")

    //建立bean对象属性与参数名的映射关系
    parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user")
    parser.addParseTarget("setIp", "IP:connection.client.host")
    parser.addParseTarget("setRequestTime", "TIME.STAMP:request.receive.time")
    parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.method")
    parser.addParseTarget("setResolution", "HTTP.URI:request.firstline.uri")
    parser.addParseTarget("setRequestProtocol", "HTTP.PROTOCOL_VERSION:request.firstline.protocol")
    parser.addParseTarget("setResponseBodyBytes", "BYTES:response.body.bytes")
    parser.addParseTarget("setReferer", "HTTP.URI:request.referer")
    parser.addParseTarget("setUserAgent", "HTTP.USERAGENT:request.user-agent")
    parser.addParseTarget("setReferDomain", "HTTP.HOST:request.referer.host")

    //返回点击流日志的解析规则
    parser
  }

  def main(args: Array[String]): Unit = {
    //2：指定解析的字符串
    val logline = "2001:980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\""

    //3：解析数据
    val record = new ClickLogBean()
    val parser =createClickLogParse()
    parser.parse(record, logline)

    println(record.getConnectionClientUser)
    println(record.getIp)
    println(record.getRequestTime)
    println(record.getMethod)
    println(record.getResolution)
    println(record.getRequestProtocol)
    println(record.getResponseBodyBytes)
    println(record.getReferer)
    println(record.getUserAgent)
    println(record.getReferDomain)
  }
}

/**
 * 拉宽后的点击流日志的样例类
 * @param uid
 * @param ip
 * @param requestTime
 * @param requestMethod
 * @param requestUrl
 * @param requestProtocol
 * @param responseStatus
 * @param responseBodyBytes
 * @param referer
 * @param userAgent
 * @param referDomain
 */
case class ClickLogWideBean(
                           @BeanProperty uid:String,            //用户id信息
                           @BeanProperty ip:String,             //ip地址
                           @BeanProperty requestTime:String,    //请求时间
                           @BeanProperty requestMethod:String,  //请求方式
                           @BeanProperty requestUrl:String,     //请求地址
                           @BeanProperty requestProtocol:String,//请求协议
                           @BeanProperty responseStatus:Int,    //响应码
                           @BeanProperty responseBodyBytes:String, //返回的数据流量
                           @BeanProperty referer:String,         //访客的来源Url
                           @BeanProperty userAgent:String,       //用户代理
                           @BeanProperty referDomain:String,     //跳转过来页面的域名
                           @BeanProperty var province:String,                  //ip对应的省份，拉宽后增加的字段
                           @BeanProperty var city:String,                      //ip对应的城市，拉宽后增加的字段
                           @BeanProperty var requestDateTime:String            //访问的时间戳，因为拉宽后的点击流数据需要写入到kafka，然后摄取到Druid，Druid需要写入的数据包含时间字段
                           )
object  ClickLogWideBean{
  def apply(clickLogBean: ClickLogBean): ClickLogWideBean =
    {
      ClickLogWideBean(
        clickLogBean.getConnectionClientUser,
        clickLogBean.getIp,
        clickLogBean.getRequestTime,
        clickLogBean.getMethod,
        clickLogBean.getResolution,
        clickLogBean.getRequestProtocol,
        clickLogBean.getRequestStatus,
        clickLogBean.getResponseBodyBytes,
        clickLogBean.getReferer,
        clickLogBean.getUserAgent,
        clickLogBean.getReferDomain,
        "",
        "",
        "")
    }
}