package common;

import java.io.DataOutputStream;

/**
 * Interface defining the service the future implementation of message processor must provide.
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public interface MessageProcessor {

    void processMessage(DataOutputStream out,String message);
    
}
