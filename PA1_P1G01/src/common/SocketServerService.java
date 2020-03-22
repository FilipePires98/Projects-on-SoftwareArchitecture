package common;

/**
 *
 * @author joaoalegria
 */
public interface SocketServerService {
    public void send(String message);
    public void close();
}
