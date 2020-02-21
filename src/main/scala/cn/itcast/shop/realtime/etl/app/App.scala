package cn.itcast.shop.realtime.etl.app

import java.util.Properties

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.process.{CartDataETL, ClickLogDataETL, CommentsDataETL, GoodsDataETL, OrderDataETL, OrderGoodsDataETL, SyncDimDataETL}
import cn.itcast.shop.realtime.etl.utils.{CanalRowDataDeserizationSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 这是实时ETL处理的入口类，所有的ETL操作都在这个对象中实现
 */
object App {
  /**
   * 入口方法
   * @param args
   */
  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1：初始化flink的运行环境
     * 2：设置flink的开发环境的并行度为1，生产环境不可以设置为1，可以不设置
     * 3：开启flink的checkpoint
     * 4：接入kafka的数据源，消费kafka中的数据
     * 5：实现所有的ETL业务
     * 6：执行任务
     */
    //TODO 1：初始化flink的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO 2：设置flink的开发环境的并行度为1，生产环境不可以设置为1，可以不设置
    env.setParallelism(1)

    //TODO 3：开启flink的checkpoint
    //开启checkpoint的时候，设置checkpoint的执行周期，每5秒钟做一次checkpoint
    env.enableCheckpointing(5000)
    //如果程序被cancel，保留以前做的checkpoint，避免数据丢失
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //同一个时间只有一个检查点，检查点的操作是否可以并行，1不并行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //指定重启策略，默认的重启策略是不停的重启
    //程序出现异常的时候会重启，重启五次，每次延迟5秒钟，如果超过了5次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000))

    //注册分布式缓存(ip资源库)
    env.registerCachedFile(GlobalConfigUtil.`ip.file.path`, "qqwry.dat")

    //TODO 4：接入kafka的数据源，消费kafka中的数据

    //TODO 5：实现所有的ETL业务
    //5.1 维度数据增加同步更新到redis中
    val syncDimDataProcess = new SyncDimDataETL(env)
    syncDimDataProcess.process()

//    //5.2 点击流日志的业务处理
//    val clickLogProcess = new ClickLogDataETL(env)
//    clickLogProcess.process()
//
//    //5.3 订单数据的业务处理
//    val orderDataProcess = new OrderDataETL(env)
//    orderDataProcess.process()

//    //5.4 订单明细表的业务处理
//    val orderGoodsDataProcess = new OrderGoodsDataETL(env)
//    orderGoodsDataProcess.process()
//
//    //5.5 商品数据表的业务操作
//    val goodsDataProcess  = new  GoodsDataETL(env)
//    goodsDataProcess.process()
//
//    //5.5 购物车数据的业务操作
//    val cartDataProcess = new CartDataETL(env)
//    cartDataProcess.process()
//
//    //5.6 评论数据的业务操作
//    val commentsDataProcess = new CommentsDataETL(env)
//    commentsDataProcess.process()

    //TODO 6：执行任务
    env.execute()
  }
}
