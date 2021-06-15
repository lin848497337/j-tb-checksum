package tb.checksum.listener;

import tb.checksum.model.Column;
import tb.checksum.model.DataMessage;
import tb.checksum.model.PKUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tb.checksum.util.DataSourceHelper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MessageCheckListener implements MessageListener{

    private static final Logger logger = LoggerFactory.getLogger(MessageCheckListener.class);
    private static final String RANGE_SELECT = "select * from {0} where {1} in ({2})";

    private DataSource dataSource;


    public void init(String ip, int port,String dbName, String username, String password, String encode)
        throws Exception {
        dataSource = DataSourceHelper.createDruidMySqlDataSource(ip, port, dbName, username, password, encode, 60, 120, null, null);
    }

    @Override
    public void onMessage(List<DataMessage> messageList) throws Exception {
        Connection conn = dataSource.getConnection();
        DataMessage message = messageList.get(0);
        List<Column> columnList = message.getColumnList();
        String tableName = message.getTableName();
        List<Column> pkList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (Column column : columnList){
            if (column.isPk()){
                pkList.add(column);
                sb.append(column.getName()).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        StringBuilder valueBuilder = new StringBuilder();
        int size = messageList.size();

        for (int i=0 ; i<size ; i++){
            valueBuilder.append("(");
            for (Column pk : pkList){
                valueBuilder.append("?,");
            }
            valueBuilder.deleteCharAt(valueBuilder.length() - 1);
            valueBuilder.append("),");
        }

        valueBuilder.deleteCharAt(valueBuilder.length() - 1);
        String crawlerSql = MessageFormat.format(RANGE_SELECT, tableName, sb.toString(), valueBuilder.toString());
        PreparedStatement ps = conn.prepareStatement(crawlerSql);
        int i=1;
        for (DataMessage msg : messageList){
            List<Object> objectList = msg.getObjectList();
            for (Column pk : pkList){
                ps.setObject(i++, objectList.get(pk.getIndex()));
            }
        }
        ResultSet rs = null;
        try{
            rs = ps.executeQuery();
        }catch (Exception e){
            e.printStackTrace();
        }
        Map<PKUnion, List<Object>> dataMap = new HashMap<>();
        while (rs.next()){
            List<Object> objectList = new ArrayList<>();
            PKUnion pkUnion = new PKUnion();
            for (Column col : columnList){
                Object value = col.javaClass().cast(rs.getObject(col.getName()));
                if (col.isPk()){
                    pkUnion.addKey(value);
                }
                objectList.add(value);
            }
            dataMap.put(pkUnion, objectList);
        }
        rs.close();
        ps.close();
        conn.close();
        check(messageList, dataMap, pkList);
    }

    private void check(List<DataMessage> sourceMessageList, Map<PKUnion, List<Object>> dataMap, List<Column> pkList){
        for (DataMessage dm : sourceMessageList){
            PKUnion pkUnion = new PKUnion();
            for (Column column : pkList){
                Object key = dm.getObjectList().get(column.getIndex());
                pkUnion.addKey(key);
            }
            List<Object> objectList = dataMap.get(pkUnion);
            if (objectList == null){
                logger.info("miss:"+dm.getTableName()+"["+pkUnion+"]");
                continue;
            }
            int size = dm.getColumnList().size();
            for (int i=0 ; i<size ; i++){
                Object srcObj = dm.getObjectList().get(i);
                Object dst = objectList.get(i);
                if (!Objects.equals(srcObj, dst)){
                    logger.info("diff:"+dm.getTableName()+"["+pkUnion+"]");
                }
            }
        }
    }
}
