package cn.itcast.shop.realtime.etl.bean

import scala.beans.BeanProperty

/**
 * 订单明细宽表的样例类
 */
case class OrderGoodsWideEntity(@BeanProperty ogId:Long,          //订单明细id
                                @BeanProperty orderId:Long,       //订单id
                                @BeanProperty goodsId:Long,       //商品id
                                @BeanProperty goodsNum:Long,      //商品数量
                                @BeanProperty goodsPrice:Double,  //商品价格
                                @BeanProperty goodsName:String,   //商品名称
                                @BeanProperty shopId:Long,        //店铺id
                                @BeanProperty goodsThirdCatId:Int,        //商品三级分类id
                                @BeanProperty goodsThirdCatName:String,   //商品三级分类名字
                                @BeanProperty goodsSecondCatId:Int,       //商品二级分类id
                                @BeanProperty goodsSecondCatName:String,  //商品二级分类名字
                                @BeanProperty goodsFirstCatId:Int,        //商品一级分类id
                                @BeanProperty goodsFirstCatName:String,   //商品一级分类名字
                                @BeanProperty areaId:Int,                 //店铺所在的区域
                                @BeanProperty shopName:String,            //店铺名字
                                @BeanProperty shopCompany:String,         //店铺公司
                                @BeanProperty cityId:Int,                 //店铺所在的城市id
                                @BeanProperty cityName:String,            //店铺所在的城市名称
                                @BeanProperty regionId:Int,               //店铺所在的大区id
                                @BeanProperty regionName:String,          //店铺所在的大区名字
                                var eventType:String                      //操作类型，insert/update/delete， 这个操作没必要加@BeanProperty，因为这个字段不需要存储，只是用来判断
                               )
