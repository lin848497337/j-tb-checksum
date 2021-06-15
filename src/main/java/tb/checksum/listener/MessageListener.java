package tb.checksum.listener;

import tb.checksum.model.DataMessage;

import java.util.List;

public interface MessageListener {
    void onMessage(List<DataMessage> messageList) throws Exception;
}
