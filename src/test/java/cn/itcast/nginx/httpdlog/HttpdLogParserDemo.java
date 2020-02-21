package cn.itcast.nginx.httpdlog;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

import java.util.List;

/**
 * 解析nginx的访问日志demo
 */
public class HttpdLogParserDemo {
    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) throws MissingDissectorsException, InvalidDissectorException, NoSuchMethodException, DissectionFailure {
        new HttpdLogParserDemo().run();
    }

    //定义解析规则
    String logFormat = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\" \"%{Addr}i\"";
    //定义数据样本
    String logLine = "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\" \"jquery-ui-theme=Eggplant; BuI=SomeThing; Apache=127.0.0.1.1351111543699529\" \"beijingshi\"";

    //3：根据定义的规则解析日志的数据
    private void run() throws MissingDissectorsException, InvalidDissectorException, NoSuchMethodException, DissectionFailure {
        printAllPossibles();
        //将日志参数映射成对象，因为后续使用方便
        Parser<MyHttpdLogRecord> parser = new HttpdLoglineParser<>(MyHttpdLogRecord.class, logFormat);

        MyHttpdLogRecord myHttpdLogRecord = new MyHttpdLogRecord();
        parser.parse(myHttpdLogRecord, logLine);
        System.out.println(myHttpdLogRecord.toString());
    }

    //4：打印出来解析的所有的参数
    private void printAllPossibles() throws MissingDissectorsException, InvalidDissectorException, NoSuchMethodException {
        //指定解析规则和解析后的对象，根据指定的解析规则来解析样本数据
        Parser<Object> parser = new HttpdLoglineParser<>(Object.class, logFormat);
        List<String> possiblesPaths = parser.getPossiblePaths();
        parser.addParseTarget(String.class.getMethod("indexOf", String.class), possiblesPaths);

        for (String path: possiblesPaths){
            System.out.println(path+"->"+parser.getCasts(path));
        }
        System.out.println("------------------------------------------------------------");
    }

}
