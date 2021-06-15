package tb.checksum.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.vendor.MySqlExceptionSorter;
import com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DataSourceHelper {

    public static Map<String, String> DEFAULT_MYSQL_CONNECTION_PROPERTIES = new HashMap<String, String>();
    static {
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("allowMultiQueries", "true"); // 多语句支持
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("rewriteBatchedStatements", "true"); // 全量目标数据源加上这个批量的参数
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("readOnlyPropagatesToServer", "false");// 关闭每次读取read-only状态,提升batch性能
//        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("allowMultiQueries", "true"); // 开启多语句能力
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("connectTimeout", "1000");
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("autoReconnect", "true");
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("zeroDateTimeBehavior", "convertToNull"); // 将0000-00-00的时间类型返回null
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("yearIsDateType", "false"); // 直接返回字符串，不做year转换date处理
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("noDatetimeStringSync", "true"); // 返回时间类型的字符串,不做时区处理
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("tinyInt1isBit", "false"); // 不处理tinyint转为bit
        // 16MB，兼容一下ADS不支持mysql，5.1.38+的server变量查询为大写的问题，人肉指定一下最大包大小
        DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("maxAllowedPacket", "1073741824");
    }

    public static DruidDataSource createDruidMySqlDataSource(String ip, int port, String dbName, String user,
                                                             String passwd, String encoding, int minPoolSize,
                                                             int maxPoolSize, Map<String, String> params,
                                                             List<String> newConnectionSQLs) throws Exception {
        DruidDataSource ds = new DruidDataSource();
        String url = "jdbc:mysql://" + ip + ":" + port;
        if (StringUtils.isNotEmpty(dbName)) {
            url = url + "/" + dbName;
        }
        url = url + "?useSSL=false";// remove warning msg
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(passwd);
        ds.setTestWhileIdle(true);
        ds.setTestOnBorrow(false);
        ds.setTestOnReturn(false);
        ds.setNotFullTimeoutRetryCount(2);
        ds.setValidConnectionCheckerClassName(MySqlValidConnectionChecker.class.getName());
        ds.setExceptionSorterClassName(MySqlExceptionSorter.class.getName());
        ds.setValidationQuery("SELECT 1");
        ds.setInitialSize(minPoolSize);
        ds.setMinIdle(minPoolSize);
        ds.setMaxActive(maxPoolSize);
        ds.setMaxWait(10 * 1000);
        ds.setTimeBetweenEvictionRunsMillis(60 * 1000);
        ds.setMinEvictableIdleTimeMillis(50 * 1000);
        ds.setUseUnfairLock(true);
        Properties prop = new Properties();
        if (StringUtils.isNotEmpty(encoding)) {
            if (StringUtils.equalsIgnoreCase(encoding, "utf8mb4")) {
                prop.put("characterEncoding", "utf8");
                newConnectionSQLs.add("set names utf8mb4");
            } else {
                prop.put("characterEncoding", encoding);
            }
        }

        prop.putAll(DEFAULT_MYSQL_CONNECTION_PROPERTIES);
        if (params != null) {
            prop.putAll(params);
        }
        ds.setConnectProperties(prop);
        if (newConnectionSQLs != null && newConnectionSQLs.size() > 0) {
            ds.setConnectionInitSqls(newConnectionSQLs);
        }
        try {
            ds.init();
        } catch (Exception e) {
            throw new RuntimeException("create druid datasource occur exception, with url : " + url + ", user : " + user
                + ", passwd : " + passwd,
                e);
        }
        return ds;
    }
}
