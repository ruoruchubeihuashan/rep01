package cn.itcast.shop.realtime.etl.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

object KafkaProps{
  /**
   * 封装kafka的属性配置
   */
  def getKafkaProperties()={
    //消费kafka数据的时候，需要指定的参数
    val properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfigUtil.`bootstrap.servers`)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GlobalConfigUtil.`group.id`)
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, GlobalConfigUtil.`enable.auto.commit`)
    properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, GlobalConfigUtil.`auto.commit.interval.ms`)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, GlobalConfigUtil.`auto.offset.reset`)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GlobalConfigUtil.`key.deserializer`)

    //将封装的kafka属性配置对象返回
    properties
  }
}
