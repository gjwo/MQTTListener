package org.ladbury.mqqtListener;

import static java.lang.Thread.sleep;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import static java.lang.Thread.sleep;

public class MQTTListener extends Thread implements MqttCallback
{
    // run control variables
    private volatile boolean msgArrived;
    private static final String DEFAULT_CLIENT_ID = "MQTT_Listener";
    private String clientId;
    private static final String DEFAULT_TOPIC = "#";
    private String topic;
    private static final int DEFAULT_QOS = 2;
    private int qos;
    private static final String DEFAULT_BROKER = "tcp://localhost:1883";
    private String broker;
    private MqttClient mqttClient;
    private int nbrMessagesReceivedOK;

    MQTTListener(String topic, String clientId, String broker, int qos)
    {
        super();
        if (topic.isEmpty()) this.topic = DEFAULT_TOPIC;
            else this.topic = topic;

        if (clientId.isEmpty()) this.clientId = DEFAULT_CLIENT_ID;
            else this.clientId = clientId;
        if (broker.isEmpty()) this.clientId = DEFAULT_BROKER;
            else this.broker = broker;
        this.qos = qos;
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

    MQTTListener()
    {
        this(DEFAULT_TOPIC,DEFAULT_CLIENT_ID,DEFAULT_BROKER,DEFAULT_QOS);
    }


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

    private static void handleMQTTException(MqttException me)
    {
        System.out.println("reason "+me.getReasonCode());
        System.out.println("msg "+me.getMessage());
        System.out.println("loc "+me.getLocalizedMessage());
        System.out.println("cause "+me.getCause());
        System.out.println("exception "+me);
        me.printStackTrace();
    }

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
