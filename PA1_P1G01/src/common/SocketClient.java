package common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom Socket Client created to provide a simple and easy object to use when needing a socket client.
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public class SocketClient {
    
    /**
     * Input stream of the socket.
     */
    private DataInputStream in;
    
    /**
     * Output stream of the socket.
     */
    private DataOutputStream out;
    
    /**
     * Instance of the communication socket to be used.
     */
    private Socket socket=null;

    /**
     * Class constructor for the client definition.
     * @param ip IP address assigned to the client.
     * @param port Port assigned to the client.
     */
    public SocketClient(String ip, int port) {
        try {
            while(this.socket==null){
                this.socket = new Socket(ip, port);
            }
            this.in = new DataInputStream( socket.getInputStream() );
            this.out = new DataOutputStream( socket.getOutputStream() );
            
        } catch (IOException ex) {
            Logger.getLogger(SocketClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Sends a text message to the subscribed socket server. 
     * @param message string containing the message to send.
     */
    public String send(String message){
        String response="";
        try {
            this.out.writeUTF(message);
            this.out.flush();
            response = this.in.readUTF();
        } catch (IOException ex) {
            Logger.getLogger(SocketClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return response;
    }
    
    /**
     * Closes the socket client.
     */
    public void close(){
        try {
            this.out.close();
            this.socket.close();
        } catch (IOException ex) {
            Logger.getLogger(SocketClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
