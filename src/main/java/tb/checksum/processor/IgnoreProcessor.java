package tb.checksum.processor;

import tb.checksum.listener.MessageListener;

import javax.sql.DataSource;

public class IgnoreProcessor implements ITableProcessor{
    @Override
    public void init(MessageListener listener, DataSource dataSource) {

    }

    @Override
    public void doTransfer() throws Exception {

    }
}
