package entities;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.*;
import kafkaUtils.EntityAction;
import message.Message;

/**
 * Class for the Report Entity for the car supervising system.
 * 
 * @author Filipe Pires (85122) and João Alegria (85048)
 */
public class ReportEntity extends JFrame implements EntityAction{
    
    private String topicName="ReportTopic";
    private FileWriter file;

    /**
     * Creates new form CollectEntity
     */
    public ReportEntity() {
        this.setTitle("Report Entiry");
        initComponents();
        
        try {
            File file=new File("src/data/REPORT.TXT");
            
            // if file doesnt exists, then create it
            if (!file.exists()) {
                    file.createNewFile();
            }
            
            this.file = new FileWriter("src/data/REPORT.TXT");
        } catch (IOException ex) {
            Logger.getLogger(ReportEntity.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void startConsumers(){
        String[] topics=new String[]{topicName};
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "REPORTGROUP");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "MessageDeserializer");
        
        kafkaUtils.Consumer<String, Message> consumer = new kafkaUtils.Consumer<>(props, topics, this);
        Thread t = new Thread(consumer);
        t.start();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 400, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 300, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        System.out.println("[Report] Running...");
        
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
                new ReportEntity().setVisible(true);
            }
        });
    }

    @Override
    public void processMessage(String topic, Object key, Object value) {
        try {
            Message data=(Message)value;
            String tmp  = data.toString();
            file.write(tmp);
            System.out.println("[REPORT] Processed message: "+tmp);
        } catch (IOException ex) {
            Logger.getLogger(ReportEntity.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
}
