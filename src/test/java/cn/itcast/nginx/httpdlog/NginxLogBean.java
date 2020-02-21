package cn.itcast.nginx.httpdlog;

import lombok.Getter;
import lombok.Setter;

/**
 * 这里的bean是解析log参数以后，赋值给的对象
 * 这里定义了很多属性，具体需要哪些属性取决于后期点击流分析的时候需要哪些字段
 */
public class NginxLogBean {
    /**
     * 这里加上了@Getter @Setter，表示代码在编译字节码的时候自动生成set和get方法
     */
    @Getter @Setter private String connectionClientUser = null;
    @Getter @Setter private String connectionClientHost = null;
    @Getter @Setter private String requestReceiveTime   = null;
    @Getter @Setter private String method               = null;
    @Getter @Setter private String referrer             = null;
    @Getter @Setter private String screenResolution     = null;
    @Getter @Setter private String requestStatus        = null;
    @Getter @Setter private String responseBodyBytes    = null;
    @Getter @Setter private Long   screenWidth          = null;
    @Getter @Setter private Long   screenHeight         = null;
    @Getter @Setter private String googleQuery          = null;
    @Getter @Setter private String bui                  = null;
    @Getter @Setter private String useragent            = null;
    @Getter @Setter private String asnNumber            = null;
    @Getter @Setter private String asnOrganization      = null;
    @Getter @Setter private String ispName              = null;
    @Getter @Setter private String ispOrganization      = null;
    @Getter @Setter private String continentName        = null;
    @Getter @Setter private String continentCode        = null;
    @Getter @Setter private String countryName          = null;
    @Getter @Setter private String countryIso           = null;
    @Getter @Setter private String subdivisionName      = null;
    @Getter @Setter private String subdivisionIso       = null;
    @Getter @Setter private String cityName             = null;
    @Getter @Setter private String postalCode           = null;
    @Getter @Setter private Double locationLatitude     = null;
    @Getter @Setter private Double locationLongitude    = null;
}
