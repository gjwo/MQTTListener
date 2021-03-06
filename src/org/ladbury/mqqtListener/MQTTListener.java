package org.ladbury.mqqtListener;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.Instant;

@SuppressWarnings("SpellCheckingInspection")
public class MQTTListener extends Thread implements MqttCallback
{
    static final String DEFAULT_CLIENT_ID = "MQTT_Listener";
    static final String DEFAULT_BROKER = "tcp://localhost:1883";
    static final String DEFAULT_TOPIC = "#";
    static final int DEFAULT_QOS = 2;

    private MqttClient mqttClient;
    private MqttConnectOptions connOpts;

    //processing variables
    private volatile boolean msgArrived;
    private volatile String lastTopic;
    private volatile MqttMessage lastPayload;
    private int nbrMessagesReceivedOK;
    private boolean api;
    private boolean echo;
    private DBRestAPI dbRestAPI;

    /**
     * MQTTListener Constructor
     * @param topic         MQTT topic to be listen to / processed
     * @param clientId      The name of this MQTT restClient
     * @param broker        The protocol, address and port of the MQTT server / broker
     * @param qos           Quality of service
     * @param user          User name for secure connections empty string if not required
     * @param password      Password for secure connections empty string if not required
     */
    MQTTListener(String topic, String clientId, String broker, int qos, String user, String password)
    {
        super();
        api = false;
        echo = false;
        if (topic.isEmpty()) topic = DEFAULT_TOPIC;
        if (clientId.isEmpty()) clientId = DEFAULT_CLIENT_ID;
        if (broker.isEmpty()) clientId = DEFAULT_BROKER;
        try
        {
            mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            if (!user.isEmpty())
            {
                connOpts.setUserName(user);
                connOpts.setPassword(password.toCharArray());
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
        try
        {
            while (!Thread.interrupted())
            {
                if (msgArrived)
                {
                    msgArrived = false;
                    nbrMessagesReceivedOK++;
                    if (isEcho()) System.out.println("Topic: '" +lastTopic+ "'"+" Message: '" + new String(lastPayload.getPayload())+ "'");
                    if (isApi()) processViaApi(lastTopic, new String(lastPayload.getPayload()));
                }
                sleep(10);
            }
        }catch (InterruptedException e)
        {
            shutdownMQTTListener();
            System.out.println("Data Processing Interrupted, exiting");
        }
    }

    private void processViaApi(String lastTopic, String message)
    {
        // expected message emon/PMon10/Upstairs_Lighting/ApparentPower' Message: '0.000 VA at 05-Sep-2017 11:21:12'
        String [] topics = lastTopic.split("/");
        int topicDepth = topics.length;
        if ((topicDepth < 4) ||
                (!topics[0].equalsIgnoreCase("emon")) ||
                (!topics[1].equalsIgnoreCase("PMon10")))return; // wrong format of topic

        String circuitName = topics[2].toLowerCase(); //Should be the circuit name Whole_House, Upstairs_Lighting etc
        String metricType = topics[3].toLowerCase(); // Should be the metrics type realPower, ApparentPower etc.
        String resource = "device/"+circuitName+"/"+metricType;

        String [] metricData = message.split(" "); //expecting "<value>", "<symbol>", "at", "<Date>", "<Time>"
        int noDataFields = metricData.length;
        if ( ((noDataFields < 4) || (!metricData[2].equalsIgnoreCase("at")))) return; // not enough data

        TimestampedDouble timestampedDouble = new TimestampedDouble(    Double.parseDouble(metricData[0]),
                                                                metricData[3]+"Z" ); //TODO fix properly
        String json = "[{" + "\"reading\":"+timestampedDouble.getValue()+"," +
                "\"timestamp\":\""+timestampedDouble.toEpochMilli()+"\"" + "}]";

        if (dbRestAPI.postToDB(resource, json)!= DBRestAPI.REST_REQUEST_SUCCESSFUL)
        {
            dbRestAPI.printLastError();
        }
    }

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
    private boolean isEcho()
    {
        return echo;
    }
    void setEcho(boolean echo)
    {
        this.echo = echo;
    }
    void setAPIDB(DBRestAPI api) { this.dbRestAPI = api;}
}
