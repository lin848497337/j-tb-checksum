package tb.checksum.model;

import java.util.ArrayList;
import java.util.List;

public class DataMessage {
    private String tableName;
    private List<Object> objectList = new ArrayList();
    private List<Column> columnList;

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Object> getObjectList() {
        return objectList;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void addValue(Object value){
        objectList.add(value);
    }
}
