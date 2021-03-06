package entities;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.*;
import kafkaUtils.Consumer;
import kafkaUtils.EntityAction;
import message.Message;
import message.MessageDeserializer;

/**
 * Class for the Batch Entity for the car supervising system.
 * This entity reads data from the BatchTopic, presents the received messages in its GUI and writes them to BATCH.TXT.
 * 
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public class BatchEntity extends JFrame implements EntityAction<Integer, Message>{
       
    /**
     * Minimum size of the consumer group definable by the user.
     */
    private final int MINGROUPSIZE = 1;
    /**
     * Maximum size of the consumer group definable by the user.
     */
    private final int MAXGROUPSIZE = 10;
    /**
     * Name of the topic that the entity reads data from.
     */
    private String topicName="BatchTopic";
    /**
     * Name of the consumer group.
     */
    private String groupName="BatchTopicGroup";
    /**
     * Consumer properties (bootstrap.servers, group.id, key.deserializer, value.deserializer, etc.).
     */
    private Properties props = new Properties();
    
    /**
     * Writer responsible for IO interactions with the file BATCH.TXT.
     */
    private FileWriter file;
    /**
     * Array of consumers dedicated to this entity (of size MAXGROUPSIZE).
     */
    private Consumer[] consumers = new Consumer[MAXGROUPSIZE];
    /**
     * Array of consumer threads, each dedicated to a consumer instance, (of size MAXGROUPSIZE).
     */
    private Thread[] consumerThreads = new Thread[MAXGROUPSIZE];
    /**
     * Number of active consumers working for the entity, definable by the user.
     */
    private int activeConsumers = 3;
    
    /**
     * Cache containing the number of times each message has been processed (to allow consumer coordination).
     */
    private Map<Integer,Integer> processedMessages = new HashMap<Integer, Integer>();
    
    /**
     * Number of know reprocessed messages.
     */
    private int reprocessed=0;
    
    /**
     * Structure with know messages.
     */
    private List<Integer> knownMessages=new ArrayList<Integer>();
    
    /**
     * Flag signaling if it is the first message.
     */
    private boolean firstMessage=true;

    /**
     * Creates new form BatchEntity and requests consumer initialization.
     */
    public BatchEntity() {
        this.setTitle("Batch Entity");
        initComponents();
        
        try {
            this.file = new FileWriter("src/data/BATCH.TXT");
        } catch (IOException ex) {
            Logger.getLogger(AlarmEntity.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        startConsumers();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        consumersLabel = new javax.swing.JLabel();
        nConsumers = new javax.swing.JSpinner();
        jScrollPane1 = new javax.swing.JScrollPane();
        logs = new javax.swing.JTextArea();
        reportAndReset = new javax.swing.JButton();
        jLabel1 = new javax.swing.JLabel();
        heartbeatBtn = new javax.swing.JCheckBox();
        speedBtn = new javax.swing.JCheckBox();
        statusBtn = new javax.swing.JCheckBox();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setSize(new java.awt.Dimension(500, 360));

        consumersLabel.setText("# of Consumers:");

        nConsumers.setModel(new SpinnerNumberModel(activeConsumers,MINGROUPSIZE,MAXGROUPSIZE,1));
        nConsumers.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                nConsumersStateChanged(evt);
            }
        });

        logs.setColumns(20);
        logs.setRows(5);
        jScrollPane1.setViewportView(logs);

        reportAndReset.setText("ReportAndReset");
        reportAndReset.setAutoscrolls(true);
        reportAndReset.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                reportAndResetMouseClicked(evt);
            }
        });

        jLabel1.setText("Message Filter:");

        heartbeatBtn.setSelected(true);
        heartbeatBtn.setText("Heartbeat");

        speedBtn.setSelected(true);
        speedBtn.setText("Speed");

        statusBtn.setSelected(true);
        statusBtn.setText("Status");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 488, Short.MAX_VALUE)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(consumersLabel)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(nConsumers, javax.swing.GroupLayout.PREFERRED_SIZE, 54, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(reportAndReset)))
                .addContainerGap())
            .addGroup(layout.createSequentialGroup()
                .addGap(15, 15, 15)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel1)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(heartbeatBtn)
                        .addGap(18, 18, 18)
                        .addComponent(speedBtn)
                        .addGap(18, 18, 18)
                        .addComponent(statusBtn)))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(consumersLabel)
                    .addComponent(nConsumers, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(reportAndReset))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel1)
                .addGap(4, 4, 4)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(heartbeatBtn)
                    .addComponent(speedBtn)
                    .addComponent(statusBtn))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 262, Short.MAX_VALUE)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    /**
     * Method through which the user defines the number of active consumers.
     * 
     * @param evt change event triggered, not used in our context
     */
    private void nConsumersStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_nConsumersStateChanged
        
        if((Integer)nConsumers.getValue() == activeConsumers) {
            return;
        }
        
        if((Integer)nConsumers.getValue() > activeConsumers) {
            String[] tmp = new String[]{topicName};
            Consumer<Integer, Message> consumer = new Consumer<Integer,Message>(consumers.length,props, tmp, this);
            Thread t = new Thread(consumer);
            t.start();
            consumers[activeConsumers] = consumer;
            consumerThreads[activeConsumers] = t;
        } else {
            consumers[(Integer)nConsumers.getValue()].shutdown();
        }
        activeConsumers = (Integer)nConsumers.getValue();
        
        String line = " consumers are listening to " + topicName;
        System.out.println("[Batch] " + activeConsumers + line);
        this.logs.append(activeConsumers + line + "\n");
    }//GEN-LAST:event_nConsumersStateChanged

    /**
     * Prints to the GUI's console the total number of processed messages of each type and resets the respective counters.
     * 
     * @param evt mouse event triggered, not used in our context
     */
    private void reportAndResetMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_reportAndResetMouseClicked
        String tmp ="";
        int total=0;
        for(int key : processedMessages.keySet()){
            total+=processedMessages.get(key);
            switch(key){
                case 0:
                    tmp+="Heartbeat: "+processedMessages.get(key)+"; ";
                    break;
                case 1:
                    tmp+="Speed: "+processedMessages.get(key)+"; ";
                    break;
                case 2:
                    tmp+="Status: "+processedMessages.get(key)+"; ";
                    break;
            }
        }
        tmp+="Reprocessed: "+reprocessed+"; ";
        tmp+="Total: "+total+"\n";

        processedMessages.clear();
        reprocessed=0;
        knownMessages.clear();
        
        processedMessages.clear();
        
        logs.append(tmp);
        logs.setCaretPosition(logs.getDocument().getLength());
        firstMessage=true;
    }//GEN-LAST:event_reportAndResetMouseClicked

    /**
     * Initializes consumers.
     */
    private void startConsumers() {                                      
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", MessageDeserializer.class.getName());
        props.put("enable.auto-commit", false);
        String[] tmp = new String[]{topicName};
        Consumer<Integer, Message> consumer;
        for(int i=0; i<(Integer)nConsumers.getValue(); i++) {
            consumer = new Consumer<Integer,Message>(i, props, tmp, this);
            Thread t = new Thread(consumer);
            t.start();
            consumers[i] = consumer;
            consumerThreads[i] = t;
        }
    } 
    /**
     * Batch entity's main method, responsible for creating and displaying the GUI.
     * Arguments are not needed.
     * 
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        
        System.out.println("[Batch] Running...");
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(CollectEntity.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(CollectEntity.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(CollectEntity.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(CollectEntity.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new BatchEntity().setVisible(true);
            }
        });
        
    }
    
    /**
     * Processes messages from the BatchTopic.
     * 
     * @param consumerId identifier of the consumer processing the current message
     * @param topic Kafka topic to which the message belongs to
     * @param key message unique key
     * @param value message value, actual message content with a format defined a priori
     */
    @Override
    public void processMessage(int consumerId, String topic, Integer key, Message value) {
        
//        // Check if the topic has just started to work (to disable updates on the number of consumers)
//        if(firstMsg) {
//            firstMsg = false;
//            nConsumers.setEnabled(firstMsg);
//        }
//        
//        // Check if the message has already been processed by a consumer
//        if(processedMsgs.containsKey(key)) {
//            
//            // Keep track of how many consumers have read the message and delete those that are save to remove
//            processedMsgs.put(key, processedMsgs.get(key)+1);
//            if(processedMsgs.get(key)==(Integer)nConsumers.getValue()) {
//                processedMsgs.remove(key);
//            }
//            
//        } else {
//            // Keep track of how many consumers have read the message (if applicable)
//            if((Integer)nConsumers.getValue() > 1) {
//                processedMsgs.put(key, 1);
//            }
//            
//            // Process message
//            int msgType = Integer.valueOf(value.toString().split("\\|")[3].trim());
//            /*
//            if(msgType==0) {
//                printedLines++;
//                System.out.println("[Batch] " + value.toString());
//                this.logs.append("[" + printedLines + "] " + value.toString() + "\n");
//            }
//            */
//            if(msgType!=4) {
////                printedLines++;
//                System.out.println("[Batch] " + value.toString());
//                this.logs.append("[" + key + "][Consumer: "+consumerId+"] " + value.toString() + "\n");
//            } else {
//                firstMsg = true;
//                nConsumers.setEnabled(firstMsg);
//                processedMsgs = new HashMap<>();
//            }
//        }

        if(firstMessage){
            logs.setText("");
            firstMessage=false;
        }
        
        try {
            String tmp  = value.toString();
            file.write(tmp+"\n");
            file.flush();
//            printedLines++;
            tmp="["+key+"][Consumer: "+consumerId+"] "+ value.toString() + "\n";
            if(value.getType()==0 && heartbeatBtn.isSelected()){
                this.logs.append(tmp);
            }
            if(value.getType()==1 && speedBtn.isSelected()){
                this.logs.append(tmp);
            }
            if(value.getType()==2 && statusBtn.isSelected()){
                this.logs.append(tmp);
            }
            logs.setCaretPosition(logs.getDocument().getLength());
//            System.out.println("[Batch] Processed message: "+tmp);
            
            if(processedMessages.containsKey(value.getType())){
                processedMessages.put(value.getType(), processedMessages.get(value.getType())+1);
            }else{
                processedMessages.put(value.getType(), 1);
            }
            
            if(knownMessages.contains(key)){
                reprocessed++;
            }else{
                knownMessages.add(key);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(BatchEntity.class.getName()).log(Level.SEVERE, null, ex);
        }

        
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JLabel consumersLabel;
    private javax.swing.JCheckBox heartbeatBtn;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTextArea logs;
    private javax.swing.JSpinner nConsumers;
    private javax.swing.JButton reportAndReset;
    private javax.swing.JCheckBox speedBtn;
    private javax.swing.JCheckBox statusBtn;
    // End of variables declaration//GEN-END:variables

}
