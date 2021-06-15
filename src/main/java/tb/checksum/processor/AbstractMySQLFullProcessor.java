package tb.checksum.processor;

import tb.checksum.listener.MessageListener;
import tb.checksum.model.Table;

import javax.sql.DataSource;

public abstract class AbstractMySQLFullProcessor implements ITableProcessor {

    protected MessageListener listener;
    protected Table table;
    protected DataSource dataSource;

    public AbstractMySQLFullProcessor(Table table) {
        this.table = table;
    }

    @Override
    public void init(MessageListener listener, DataSource dataSource) {
        this.listener = listener;
        this.dataSource = dataSource;

    }

    public void transfer() {
        try {
            this.doTransfer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
