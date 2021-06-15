package tb.checksum;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.mysql.cj.MysqlType;
import tb.checksum.model.Column;
import tb.checksum.model.Table;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class TableMetaUtil {

    public static List<String> queryTableList(DataSource dataSource) throws SQLException {
        Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("show tables");
        ResultSet rs = ps.executeQuery();
        List<String> tableList = new ArrayList<>();
        while (rs.next()){
            String tableName = rs.getString(1);
            tableList.add(tableName);
        }
        rs.close();
        ps.close();
        conn.close();
        return tableList;
    }

    public static Table buildTableMeta(String table, DataSource dataSource) throws Exception {
        Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(MessageFormat.format("show create table {0}",table));
        ResultSet rs = ps.executeQuery();
        String ddl = null;
        while (rs.next()){
            ddl = rs.getString(2);
        }
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(ddl, DbType.mysql);
        SQLCreateTableStatement createTableStatement = parser.parseCreateTable();
        List<String> pks = createTableStatement.getPrimaryKeyNames();
        List<SQLColumnDefinition> definitions = createTableStatement.getColumnDefinitions();
        List<Column> columnList = new ArrayList<>();
        List<Column> pkList = new ArrayList<>();
        int index = 0;
        for (SQLColumnDefinition definition : definitions){
            Column column = new Column();
            column.setIndex(index++);
            column.setName(unwrap(definition.getColumnName()));
            column.setPk(definition.isPrimaryKey());
            MysqlType mysqlType = MysqlType.getByJdbcType(definition.getDataType().jdbcType());
            column.setType(mysqlType);
            columnList.add(column);
            if (column.isPk()){
                pkList.add(column);
            }
        }
        Table tab = new Table();
        tab.setTableName(table);
        tab.setColumnList(columnList);
        tab.setPkList(pkList);
        rs.close();
        ps.close();
        conn.close();
        return tab;
    }

    private static String unwrap(String columnName){
        if (columnName.startsWith("`")){
            return columnName.substring(1, columnName.length() - 1);
        }
        return columnName;
    }
}
