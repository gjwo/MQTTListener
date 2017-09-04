package org.ladbury.mqqtListener;

import static java.lang.Thread.sleep;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import static java.lang.Thread.sleep;

public class MQTTListener extends Thread implements MqttCallback
{
    static final String DEFAULT_CLIENT_ID = "MQTT_Listener";
    static final String DEFAULT_BROKER = "tcp://localhost:1883";
    static final String DEFAULT_TOPIC = "#";
    static final int DEFAULT_QOS = 2;

    //MQTT set up variables
    private String clientId;
    private String topic;
    private int qos;
    private String broker;
    private String user = "";
    private String password = "";
    private MqttClient mqttClient;
    private MqttConnectOptions connOpts;

    //processing variables
    private volatile boolean msgArrived;
    private volatile String lastTopic;
    private volatile MqttMessage lastPayload;
    private int nbrMessagesReceivedOK;
    private boolean api;
    private boolean trace;

    /**
     * MQTTListener Constructor
     * @param topic         MQQT topic to be listen to / processed
     * @param clientId      The name of this MQTT client
     * @param broker        The protocol, address and port of the MQTT server / broker
     * @param qos           Quality of service
     * @param user          User name for secure connections empty string if not required
     * @param password      Password for secure connections empty string if not required
     */
    MQTTListener(String topic, String clientId, String broker, int qos, String user, String password)
    {
        super();
        api = false;
        trace = false;
        if (topic.isEmpty()) this.topic = DEFAULT_TOPIC;
            else this.topic = topic;
        if (clientId.isEmpty()) this.clientId = DEFAULT_CLIENT_ID;
            else this.clientId = clientId;
        if (broker.isEmpty()) this.clientId = DEFAULT_BROKER;
            else this.broker = broker;
        this.qos = qos;
        this.user = user;
        this.password = password;
        try
        {
            mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            if (!this.user.isEmpty())
            {
                connOpts.setUserName(this.user);
                connOpts.setPassword(this.password.toCharArray());
            }
            System.out.println("Connecting MQTTListener to broker: "+broker);
            mqttClient.connect(connOpts);
            System.out.println("MQTTListener Connected");
            mqttClient.subscribe(topic, qos);
        } catch (MqttException e)
        {
            e.printStackTrace();
            System.exit(2);
        }
    }

    /**
     *  MQTTListener Constructor with default parameters
     */
    MQTTListener()
    {
        this(DEFAULT_TOPIC,DEFAULT_CLIENT_ID,DEFAULT_BROKER,DEFAULT_QOS, "", "");
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
                    msgArrived = false;
                    nbrMessagesReceivedOK++;
                    if (isTrace()) System.out.println("| Topic: '" +lastTopic+ "'"+" Message: '" + new String(lastPayload.getPayload())+ "'");
                    if (isApi()) processViaApi(lastTopic, lastPayload.getPayload());
                }
                sleep(100);
            }
        }catch (InterruptedException e)
        {
            shutdownMQTTListener();
            System.out.println("Data Processing Interrupted, exiting");
        }
    }
    private void processViaApi(String lastTopic, byte[] payload)
    {
    }

    // Implementation of MqttCallback interface

    /**
     * connectionLost       called back if the connection is lost
     * @param throwable     thrown if we cannot reconnect
     */
    @Override
    public void connectionLost(Throwable throwable)
    {
        System.out.println("Listener connection to broker lost! Trying to reconnect");
        // code to reconnect to the broker would go here if desired
        try
        {
            mqttClient.connect(connOpts);
            System.out.println("Listener connection reconnected to broker");

        } catch (MqttException me)
        {
            handleMQTTException(me);
            System.out.println("Listener connection to broker lost! Reconnect failed");
            System.exit(9);
        }

    }

    /**
     * messageArrived       Called when there is a message to be processed
     * @param topic         message topic
     * @param mqttMessage   Message including payload
     * @throws Exception    not thrown
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception
    {
        msgArrived = true;
        lastTopic = topic;
        lastPayload = mqttMessage;
    }

    /**
     * deliveryComplete             Called when a message we sent has been completely received (not used)
     * @param iMqttDeliveryToken    A token identifying the message sent
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken)
    {

    }

    //Utility stuff

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

    /**
     * handleMQTTException  A method to print out any MQTT exception tidily
     * @param me            The exception to be handled
     */
    private static void handleMQTTException(MqttException me)
    {
        System.out.println("reason "+me.getReasonCode());
        System.out.println("msg "+me.getMessage());
        System.out.println("loc "+me.getLocalizedMessage());
        System.out.println("cause "+me.getCause());
        System.out.println("exception "+me);
        me.printStackTrace();
    }
    // getters and setters
    private boolean isApi()
    {
        return api;
    }
    void setApi(boolean api)
    {
        this.api = api;
    }
    private boolean isTrace()
    {
        return trace;
    }
    void setTrace(boolean trace)
    {
        this.trace = trace;
    }
}
