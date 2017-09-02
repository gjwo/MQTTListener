package org.ladbury.mqqtListener;

import static java.lang.Thread.sleep;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import static java.lang.Thread.sleep;

public class MQTTListener extends Thread implements  Runnable, MqttCallback
{
    // run control variables
    private volatile boolean msgArrived;
    private volatile boolean stop;

    @Override
    public void connectionLost(Throwable throwable)
    {
        System.out.println("Subscriber connection lost!");
        // code to reconnect to the broker would go here if desired

    }
    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception
    {
        msgArrived = true;
        System.out.println("| Topic: '" +s+ "'"+" Message: '" + new String(mqttMessage.getPayload())+ "'");
    }
    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken)
    {

    }

    private String baseTopic;
    private String clientId;
    private String topic;
    private int qos;
    private String broker;
    private MqttClient mqttClient;
    private int nbrMessagesReceivedOK;

    public MQTTListener()
    {
        baseTopic   = "emon";
        clientId    = "PMon10";
        topic       = baseTopic +"/"+clientId;
        qos         = 2;
        broker      = "tcp://localhost:1883";

        try
        {
            mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting MQTTListener to broker: "+broker);
            mqttClient.connect(connOpts);
            System.out.println("MQTTListener Connected");
            mqttClient.subscribe("emon/#", qos);
        } catch (MqttException e)
        {
            e.printStackTrace();
            System.exit(2);
        }
    }

    /**
     * shutdownMQTTListener   Tidy shutdown of the processor
     */
    private void shutdownMQTTListener()
    {
        try
        {
            mqttClient.disconnect();
            System.out.println("Disconnected");
        } catch (MqttException me)
        {
            handleMQTTException(me);
        }
        System.out.println("MQTTListener Disconnected");
        System.out.println(nbrMessagesReceivedOK+ " messages Received successfully");
    }

    private void handleMQTTException(MqttException me)
    {
        System.out.println("reason "+me.getReasonCode());
        System.out.println("msg "+me.getMessage());
        System.out.println("loc "+me.getLocalizedMessage());
        System.out.println("cause "+me.getCause());
        System.out.println("exception "+me);
        me.printStackTrace();
    }
    //
    // Runnable implementation
    //

    /**
     * run  The main processing loop
     */
    @Override
    public void run()
    {
        byte[] serialBytes = null;
        String subTopic;
        try
        {
            while (!Thread.interrupted())
            {
                if (msgArrived)
                {
                    nbrMessagesReceivedOK++;
                    msgArrived = false;
                }
                sleep(100);
            }
        }catch (InterruptedException e)
        {
            shutdownMQTTListener();
            System.out.println("Data Processing Interrupted, exiting");
        }
    }
}
