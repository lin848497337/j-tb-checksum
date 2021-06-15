package tb.checksum.processor;

import tb.checksum.model.Column;
import tb.checksum.model.DataMessage;
import tb.checksum.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class MySQLFullExtractorStreamProcessor extends AbstractMySQLFullProcessor{

    private static final String COUNT = "select count(*) from {0}";
    private static final String SOURCE_QUERY = "select *  from {0} ";
    private static final Logger logger = LoggerFactory.getLogger(MySQLFullExtractorStreamProcessor.class);
    private long count;

    public MySQLFullExtractorStreamProcessor(Table table) {
        super(table);
    }

    @Override
    public void doTransfer() throws Exception{
        Connection connection = dataSource.getConnection();
        logger.info(MessageFormat.format("begin to transfer {0}", table.getTableName()));
        PreparedStatement countPs = connection.prepareStatement(MessageFormat.format(COUNT, table.getTableName()));
        ResultSet countRs = countPs.executeQuery();
        while (countRs.next()){
            long count = countRs.getLong(1);
            this.count = count;
        }
        countRs.close();
        countPs.close();
        Progress progress = new Progress(count, 5);
        String crawlerSql = MessageFormat.format(SOURCE_QUERY, table.getTableName());
        PreparedStatement ps = connection.prepareStatement(crawlerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = ps.executeQuery();
        final int batchSize = 1000;
        List<DataMessage> messageList = new ArrayList<>(batchSize);
        List<Column> columnList = table.getColumnList();
        long transferCount = 0;
        while (rs.next()){
            DataMessage message = new DataMessage();
            message.setColumnList(columnList);
            message.setTableName(table.getTableName());
            for (Column column : columnList){
                Object value = rs.getObject(column.getName());
                message.addValue(column.javaClass().cast(value));
            }
            messageList.add(message);
            if (messageList.size() == 1000){
                listener.onMessage(messageList);
                messageList = new ArrayList<>(batchSize);
                progress.setCur(transferCount);
                if (progress.tryPrint()){
                    logger.info(MessageFormat.format("transfer table progress {0}, transfer count {1}, {2}", progress.toString(), transferCount, table.getTableName()));
                }
            }
            transferCount ++;
        }

        if (!messageList.isEmpty()){
            listener.onMessage(messageList);
            progress.setCur(transferCount);
            if (progress.tryPrint()){
                logger.info(MessageFormat.format("transfer table progress {0}, transfer count {1}, {2}", progress.toString(), transferCount, table.getTableName()));
            }
        }
        logger.info(MessageFormat.format("success to transfer {0}", table.getTableName()));
        rs.close();
        ps.close();
        connection.close();
    }
}
