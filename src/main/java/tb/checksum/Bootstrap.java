package tb.checksum;

import tb.checksum.listener.MessageCheckListener;

import java.io.InputStream;
import java.util.Properties;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        InputStream in = Bootstrap.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(in);
        String ipSource = properties.getProperty("ipSource");
        Integer portSource = Integer.valueOf(properties.getProperty("portSource"));
        String usernameSource = properties.getProperty("usernameSource");
        String passwordSource = properties.getProperty("passwordSource");
        String dbNameSource = properties.getProperty("dbNameSource");

        String ipDst = properties.getProperty("ipDst");
        Integer portDst = Integer.valueOf(properties.getProperty("portDst"));
        String usernameDst = properties.getProperty("usernameDst");
        String passwordDst =properties.getProperty("passwordDst");
        String dbNameDst = properties.getProperty("dbNameDst");
        int poolSize = 10;
        Extractor extractor = new Extractor();
        MessageCheckListener checkListener = new MessageCheckListener();
        checkListener.init(ipDst, portDst, dbNameDst, usernameDst, passwordDst, "utf8");
        extractor.init(ipSource, portSource, dbNameSource, usernameSource, passwordSource, "utf8", poolSize, checkListener);
        extractor.doCheck();
    }
}
