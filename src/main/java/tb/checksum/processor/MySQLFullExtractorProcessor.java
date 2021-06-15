package tb.checksum.processor;

import tb.checksum.model.Column;
import tb.checksum.model.DataMessage;
import tb.checksum.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class MySQLFullExtractorProcessor extends AbstractMySQLFullProcessor{

    private static final Logger logger = LoggerFactory.getLogger(MySQLFullExtractorProcessor.class);

    private static final String COUNT = "select count(*) from {0}";
    private static final String CRAWLER = "select * from {0} where {1} > {2,number,#} order by {3} limit 1000";
    private static final String BORDER_QUERY = "select max({0}) as maxPk, min({1}) as minPk from {2}";
    private long count;
    private long minPk;
    private long maxPk;

    public MySQLFullExtractorProcessor(Table table) {
        super(table);
    }

    private void initScope() throws SQLException {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement(MessageFormat.format(COUNT, table.getTableName()));
        ResultSet rs = ps.executeQuery();
        while (rs.next()){
            long count = rs.getLong(1);
            this.count = count;
        }
        rs.close();
        ps.close();
        Column column = table.getPkList().get(0);
        PreparedStatement borderPs = connection.prepareStatement(MessageFormat.format(BORDER_QUERY, column.getName(), column.getName(), table.getTableName()));
        ResultSet borderSet = borderPs.executeQuery();
        while (borderSet.next()){
            minPk = borderSet.getLong("minPk") - 1;
            maxPk = borderSet.getLong("maxPk");
        }
        borderSet.close();
        borderPs.close();
        connection.close();
        logger.info(MessageFormat.format("init scope for {0} min {1} max {2}", table.getTableName(), minPk, maxPk));
    }

    private String getCrawler(){
        Column column = table.getPkList().get(0);
        return MessageFormat.format(CRAWLER, table.getTableName(), column.getName(), minPk, column.getName());
    }

    @Override
    public void doTransfer() throws Exception{
        initScope();
        Connection connection = dataSource.getConnection();
        logger.info(MessageFormat.format("begin to transfer {0}", table.getTableName()));
        Progress progress = new Progress(count, 5);
        int transferCount = 0;
        while (minPk < maxPk){
            String crawlerSql = getCrawler();
            PreparedStatement ps = connection.prepareStatement(crawlerSql);
            ResultSet rs = ps.executeQuery();
            List<Column> columnList = table.getColumnList();
            List<DataMessage> messageList = new ArrayList<>();
            while (rs.next()){
                DataMessage message = new DataMessage();
                message.setColumnList(columnList);
                message.setTableName(table.getTableName());
                for (Column column : columnList){
                    if (column.isPk()){
                        Object v = column.javaClass().cast(rs.getObject(column.getName()));
                        Long value = rs.getLong(column.getName());
                        minPk = Math.max(value, minPk);
                        message.addValue(v);
                    }else{
                        Object value = column.javaClass().cast(rs.getObject(column.getName()));
                        message.addValue(value);
                    }
                }
                transferCount ++;
                progress.setCur(transferCount);
                if (progress.tryPrint()){
                    logger.info(MessageFormat.format("transfer table progress {0}, transfer count {1}", progress.toString(), transferCount));
                }
                messageList.add(message);
            }
            if (!messageList.isEmpty()){
                listener.onMessage(messageList);
            }else{
                break;
            }
        }
        logger.info(MessageFormat.format("success to transfer {0}", table.getTableName()));
        connection.close();
    }
}
