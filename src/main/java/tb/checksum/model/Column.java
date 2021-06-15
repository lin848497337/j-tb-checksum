package tb.checksum.model;

import com.mysql.cj.MysqlType;

public class Column {
    private String name;
    private MysqlType type;
    private boolean isPk;
    private int index;
    private Class typeClass;

    public String getName() {
        return name;
    }

    public Class javaClass(){
        return typeClass;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MysqlType getType() {
        return type;
    }

    public void setType(MysqlType type) throws ClassNotFoundException {
        this.type = type;
        this.typeClass = Class.forName(type.getClassName());
    }

    public boolean isPk() {
        return isPk;
    }

    public void setPk(boolean pk) {
        isPk = pk;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
