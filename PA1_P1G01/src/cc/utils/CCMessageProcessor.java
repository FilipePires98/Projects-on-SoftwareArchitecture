package cc.utils;
import cc.UiAndMainControlsCC;
import common.MessageProcessor;
import common.SocketServerService;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class responsible for processing all the received messages from the Farm Infrastructure.
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public class CCMessageProcessor implements MessageProcessor{
    
    private class ProcessingThread implements Runnable {

        /**
         * Message to be handled.
         */
        private String message;
        
        private DataOutputStream out;

        /**
         * Constructor for the thread definition.
         * @param message Message to be handled.
         */
        public ProcessingThread(DataOutputStream out, String message) {
            this.message = message;
            this.out=out;
        }

        /**
         * Main method defining the life-cycle of the message processing Thread.
         */
        @Override
        public void run() {
            
        }
    }
    
    /**
     * Instance of the Control Center whose messages are to be processed.
     */
    private UiAndMainControlsCC cc;
    
    /**
     * Class constructor where the Control Center whose messages are to be processed is defined.
     * @param cc Instance of the Control Center whose messages are to be processed.
     */
    public CCMessageProcessor(UiAndMainControlsCC cc) {
        this.cc=cc;
    }
    
    /**
     * Processes the incoming messages in a sequential manner since the Control Center needs to process each message one at a time.
     * @param message formatted message with the code of what to execute.
     */

    
    
    
    @Override
    public void processMessage(SocketServerService out, String message) {
//        Thread processor = new Thread(new ProcessingThread(out, message));
//        processor.start();
        String[] processedMessage = message.split(";");
        switch(processedMessage[0]){
            case "presentInStorehouse":
                cc.presentFarmerInStorehouse(Integer.valueOf(processedMessage[1]), Integer.valueOf(processedMessage[2]));
                out.send("MessageProcessed");
                break;
            case "presentInStanding":
                cc.presentFarmerInStandingArea(Integer.valueOf(processedMessage[1]), Integer.valueOf(processedMessage[2]));
                out.send("MessageProcessed");
                break;
            case "presentInPath":
                cc.presentFarmerInPath(Integer.valueOf(processedMessage[1]), Integer.valueOf(processedMessage[2]), Integer.valueOf(processedMessage[3]));
                out.send("MessageProcessed");
                break;
            case "presentInGranary":
                cc.presentFarmerInGranary(Integer.valueOf(processedMessage[1]), Integer.valueOf(processedMessage[2]));
                out.send("MessageProcessed");
                break;
            case "presentInCollecting":
                cc.presentCollectingFarmer(Integer.valueOf(processedMessage[1]));
                out.send("MessageProcessed");
                break;
            case "presentInStoring":
                cc.presentStoringFarmer(Integer.valueOf(processedMessage[1]));
                out.send("MessageProcessed");
                break;
            case "updateGranaryCobs":
                cc.updateGranaryCornCobs(Integer.valueOf(processedMessage[1]));
                out.send("MessageProcessed");
                break;
            case "updateStorehouseCobs":
                cc.updateStorehouseCornCobs(Integer.valueOf(processedMessage[1]));
                out.send("MessageProcessed");
                break;
            case "infrastructureServerOnline":
                out.send("MessageProcessed");
                cc.initFIClient();
                break;
//            case "allFarmersrReadyToStart":
//                this.cc.enableStartBtn();
//                break;
//            case "allFarmersrReadyToCollect":
//                this.cc.enableCollectBtn();
//                break;
//            case "allFarmersrReadyToReturn":
//                this.cc.enableReturnBtn();
//                break;
//            case "allFarmersrReadyWaiting":
//                this.cc.enablePrepareBtn();
//                break;
            case "endSimulationOrder":
                out.send("MessageProcessed");
                out.close();
                cc.closeSocketClient();
                cc.close();
                break;
        }

    }
}
