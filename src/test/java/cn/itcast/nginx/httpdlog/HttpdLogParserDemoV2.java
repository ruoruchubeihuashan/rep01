package cn.itcast.nginx.httpdlog;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

import java.util.List;

/**
 * 根据刚才的例子可以发现，将log日志中的参数可以解析出来，并进行了打印
 * 需要将所有的解析后的参数赋值给JavaBean，因为JavaBean应用起来方便
 */
public class HttpdLogParserDemoV2 {
    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) throws MissingDissectorsException, InvalidDissectorException, NoSuchMethodException, DissectionFailure {
        new HttpdLogParserDemoV2().run();
    }

    //定义解析规则
    String logFormat = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\" \"%{Addr}i\"";
    //定义数据样本
    String logLine = "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\" \"jquery-ui-theme=Eggplant; BuI=SomeThing; Apache=127.0.0.1.1351111543699529\" \"beijingshi\"";

    //3：根据定义的规则解析日志的数据
    private void run() throws MissingDissectorsException, InvalidDissectorException, NoSuchMethodException, DissectionFailure {
        //将日志参数映射成对象，因为后续使用方便
        Parser<NginxLogBean> parser = new HttpdLoglineParser<>(NginxLogBean.class, logFormat);
        parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user");
        parser.addParseTarget("setConnectionClientHost", "IP:connection.client.host");

        NginxLogBean nginxLogBean = new NginxLogBean();
        parser.parse(nginxLogBean, logLine);
        System.out.println(nginxLogBean.toString());
        System.out.println("---------------------------------------------------");
        System.out.println(nginxLogBean.getConnectionClientUser());
        System.out.println(nginxLogBean.getConnectionClientHost());
    }
}
