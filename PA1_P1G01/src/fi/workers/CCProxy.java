package fi.workers;

import common.SocketClient;
import fi.utils.EndSimulationException;
import fi.FarmInfrastructure;
import fi.utils.StopHarvestException;
import fi.ccInterfaces.GranaryCCInt;
import fi.ccInterfaces.PathCCInt;
import fi.ccInterfaces.StandingCCInt;
import fi.ccInterfaces.StorehouseCCInt;
import common.MessageProcessor;
import common.SocketServerService;
import fi.UiAndMainControlsFI;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Message processor for the Farm Infrastructure.
 * 
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public class CCProxy implements MessageProcessor {

    /**
     * Internal class used to specify the life-cycle of the message processing
     * Thread.
     */
    private class ProcessingThread implements Runnable {

        /**
         * Message to be handled.
         */
        private final String message;
        
        private final DataOutputStream out;

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
            try {
                String[] processedMessage = this.message.split(";");
                switch (processedMessage[0]) {
                    case "waitSimulationReady":
                        storeHouse.waitAllFarmersReady();
                        out.writeUTF("allFarmersReadyWaiting");
                        break;
                    case "prepareOrder":
                        storeHouse.sendSelectionAndPrepareOrder(Integer.valueOf(processedMessage[1]),
                                Integer.valueOf(processedMessage[2]), Integer.valueOf(processedMessage[3]),
                                Integer.valueOf(processedMessage[4]));
                        standing.waitForAllFarmers();
                        out.writeUTF("allFarmersReadyToStart");
                        break;
                    case "startHarvestOrder":
                        standing.sendStartOrder();
                        granary.waitAllFarmersReadyToCollect();
                        out.writeUTF("allFarmersReadyToCollect");
                        break;
                    case "collectOrder":
                        granary.sendCollectOrder();
                        granary.waitAllFarmersCollect();
                        out.writeUTF("allFarmersReadyToReturn");
                        break;
                    case "returnOrder":
                        granary.sendReturnOrder();
                        storeHouse.waitAllFarmersReady();
                        out.writeUTF("allFarmersReadyWaiting");
                        break;
                    case "stopHarvestOrder":
                        storeHouse.control("stopHarvest");
                        standing.control("stopHarvest");
                        path.control("stopHarvest");
                        granary.control("stopHarvest");
                        storeHouse.waitAllFarmersReady();
                        out.writeUTF("allFarmersReadyWaiting");
                        break;
                    case "endSimulationOrder":
                        storeHouse.control("endSimulation");
                        standing.control("endSimulation");
                        path.control("endSimulation");
                        granary.control("endSimulation");
                        out.writeUTF("MessageProcessed");
                        out.close();
                        fi.closeSocketClient();
                        fi.close();

                        break;
                }
            } catch (StopHarvestException ex) {
                System.out.println("BANANANA");
                try {
                    out.writeUTF("Unsuccessfull Action");
                } catch (IOException ex1) {
                    Logger.getLogger(CCProxy.class.getName()).log(Level.SEVERE, null, ex1);
                }
            } catch (EndSimulationException ex) {
                try {
                    out.writeUTF("Unsuccessfull Action");
                } catch (IOException ex1) {
                    Logger.getLogger(CCProxy.class.getName()).log(Level.SEVERE, null, ex1);
                }
            } catch (IOException ex) {
                Logger.getLogger(CCProxy.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Instance of the storehouse area.
     */
    private StorehouseCCInt storeHouse;
    /**
     * Instance of the standing area.
     */
    private StandingCCInt standing;
    /**
     * Instance of the path area.
     */
    private PathCCInt path;
    /**
     * Instance of the granary area.
     */
    private GranaryCCInt granary;
    
    /**
     * Instance of the Farm Infrastructure whose messages are to be processed.
     */
    private UiAndMainControlsFI fi;

    /**
     * Class constructor where the Control Center whose messages are to be processed is defined.
     * @param cc Instance of the Control Center whose messages are to be processed.
     */
    /**
     * Class constructor where the Farm Infrastructure whose messages are to be processed and the respective Farm Areas are defined.
     * @param fi Instance of the Farm Infrastructure whose messages are to be processed.
     * @param storeHouse Instance of the storehouse area.
     * @param standing Instance of the standing area.
     * @param path Instance of the path area.
     * @param granary Instance of the granary area.
     */
    public CCProxy(UiAndMainControlsFI fi, StorehouseCCInt storeHouse, StandingCCInt standing, PathCCInt path,
            GranaryCCInt granary) {
        this.storeHouse = storeHouse;
        this.standing = standing;
        this.path = path;
        this.granary = granary;
        this.fi = fi;

    }

    /**
     * Processes the incoming messages. For each message a new Thread is started
     * since the Farm Infrastructure needs to process several messages at the same
     * time.
     * 
     * @param message string containing the message to process
     */
    @Override
    public void processMessage(DataOutputStream out, String message) {
        Thread processor = new Thread(new ProcessingThread(out, message));
        processor.start();
    }

}
