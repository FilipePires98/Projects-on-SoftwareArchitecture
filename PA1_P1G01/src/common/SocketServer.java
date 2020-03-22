package common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public class SocketServer implements Runnable, SocketServerService{
    
    private Socket socket;
    
    /**
     * Input stream of the socket.
     */
    private DataInputStream in;
    
    /**
     * Output stream of the socket.
     */
    private DataOutputStream out;
    
    /**
     * Port assigned to the server.
     */
    private int port;
    
    /**
     * Instance of the message processor assigned to the server.
     */
    private MessageProcessor mp;

    /**
     * Class constructor for the server definition.
     * @param port IP address assigned to the server.
     * @param mp Port assigned to the server.
     */
    public SocketServer(int port, MessageProcessor mp) {
        this.port=port;
        this.mp = mp;
    }
    
    /**
     * Executes the life-cycle of the socket server.
     * When receiving a new message the server passes the message to the respective message processor.
     */
    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(this.port);
            this.socket = serverSocket.accept();
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());
            String receivedMessage="a";
            while(!receivedMessage.equals("endSimulationOrder")){
                receivedMessage=this.in.readUTF();
                System.out.println("Transmitted Message: "+receivedMessage);
                this.mp.processMessage(this,receivedMessage);
            }
        } catch (IOException ex) {
            Logger.getLogger(SocketServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void send(String message){
        try {
            this.out.writeUTF(message);
        } catch (IOException ex) {
            Logger.getLogger(SocketServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void close(){
        try {
            this.socket.close();
        } catch (IOException ex) {
            Logger.getLogger(SocketServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
