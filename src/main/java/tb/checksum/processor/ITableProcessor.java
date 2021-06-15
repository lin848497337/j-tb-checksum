package tb.checksum.processor;

import tb.checksum.listener.MessageListener;

import javax.sql.DataSource;

public interface ITableProcessor {
    void init(MessageListener listener, DataSource dataSource);
    void doTransfer() throws Exception;
}
