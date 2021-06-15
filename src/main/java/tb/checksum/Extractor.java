package tb.checksum;

import com.mysql.cj.MysqlType;
import tb.checksum.listener.MessageListener;
import tb.checksum.model.Column;
import tb.checksum.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tb.checksum.processor.ITableProcessor;
import tb.checksum.processor.IgnoreProcessor;
import tb.checksum.processor.MySQLFullExtractorProcessor;
import tb.checksum.processor.MySQLFullExtractorStreamProcessor;
import tb.checksum.util.DataSourceHelper;

import javax.sql.DataSource;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Extractor {

    private static final Logger logger = LoggerFactory.getLogger(Extractor.class);
    private List<String> tableList;
    private List<ITableProcessor> processorList = new ArrayList<>();
    private ExecutorService executorService;
    private MessageListener listener;
    private DataSource dataSource;

    public void init(String ip, int port, String dbName, String username, String password , String encode,int poolSize,MessageListener messageListener)
        throws Exception {
        ;
        this.listener = messageListener;
        dataSource = DataSourceHelper
            .createDruidMySqlDataSource(ip, port, dbName, username, password, encode, poolSize, poolSize+20, null, null);
        executorService = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "extractor");
            t.setDaemon(true);
            return t;
        });
        tableList = TableMetaUtil.queryTableList(dataSource);
    }

    public void buildProcessor() throws Exception {
        for (String table : tableList){
            ITableProcessor processor = chooseProcessor(table);
            processor.init(listener, dataSource);
            processorList.add(processor);
        }
    }

    public ITableProcessor chooseProcessor(String table) throws Exception {
        Table tableMeta = TableMetaUtil.buildTableMeta(table, dataSource);
        if (tableMeta.getPkList().isEmpty()){
            logger.warn(MessageFormat.format("ignore has no pk table {0}", table));
            return new IgnoreProcessor();
        }
        List<Column> columnList = tableMeta.getPkList();
        if (columnList == null || columnList.size() != 1){
            return new MySQLFullExtractorStreamProcessor(tableMeta);
        }else {
            Column column = columnList.get(0);
            MysqlType pkType = column.getType();
            try {
                Class cls = Class.forName(pkType.getClassName());
                if (cls.equals(Long.class) ||
                cls.equals(Integer.class) ||
                cls.equals(Short.class) ||
                cls.equals(Byte.class)){
                    return new MySQLFullExtractorProcessor(tableMeta);
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return new MySQLFullExtractorStreamProcessor(tableMeta);
        }
    }


    public void doCheck() throws Exception {
        buildProcessor();
        ExecutorTemplate template = new ExecutorTemplate(executorService);
        for (final ITableProcessor processor : processorList){
            template.submit(() -> {
                try {
                    processor.doTransfer();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        template.waitForResult();
        logger.info("all table transfer down!");
    }
}
