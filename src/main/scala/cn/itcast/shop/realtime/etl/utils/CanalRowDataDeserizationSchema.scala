package cn.itcast.shop.realtime.etl.utils

import cn.itcast.canal.bean.RowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

class CanalRowDataDeserizationSchema extends  AbstractDeserializationSchema[RowData]{
  /**
   * 对rowData进行反序列化
   * @param message 字节码数据，是在kafka中读取到的
   * @return 将字节码转换成RowData对象返回
   */
  override def deserialize(message: Array[Byte]): RowData = {
      //需要将字节码数据转换成对象返回
      new RowData(message)
  }
}
