package tb.checksum.model;

import java.util.List;

public class Table {
    private String tableName;
    private List<Column> columnList;
    private List<Column> pkList;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public List<Column> getPkList() {
        return pkList;
    }

    public void setPkList(List<Column> pkList) {
        this.pkList = pkList;
    }
}
