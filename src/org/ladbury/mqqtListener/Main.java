package org.ladbury.mqqtListener;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
public class Main implements IParameterValidator
{
    //private JCommander jc;
    private int numberArgs =0;
    private static Main main;
    @Parameter(names = {"--help", "-h",},description = "Display help information", help = true)
    private boolean help=false;
    @Parameter(names={"--topic","-t" }, description = "MQTT topic maintopic/subtopic/etc ")
    private String topic = MQTTListener.DEFAULT_TOPIC;
    @Parameter(names={"--client","-c" }, description = "The name of this MQTT client")
    private String clientId = MQTTListener.DEFAULT_CLIENT_ID;
    @Parameter(names={"--broker","-b" },description = "The protocol, address and port of the broker, e.g.tcp://localhost:1883")
    private String broker = MQTTListener.DEFAULT_BROKER;
    @Parameter(names={"--qos","-q" }, description = "0: The broker/client will deliver the message once, with no confirmation.\n"+
                                                    "1: The broker/client will deliver the message at least once, with confirmation required.\n"+
                                                    "2: The broker/client will deliver the message exactly once by using a four step handshake.")
    private int qos = MQTTListener.DEFAULT_QOS;
    @Parameter(names={"--user","-u" }, description = "The user name for logging into the MQTT broker")
    private String user = "";
    @Parameter(names={"--password","-p" }, description = "The password for logging into the MQTT broker")
    private String password = "";
    @Parameter(names={"--echo","-e" }, description = "if present output a echo what is being seen message")
    private boolean echo = false;
    @Parameter(names={"--api","-a" }, description = "if present output the message to the api")
    private boolean api = false;
    private DBRestAPI dbRestAPI = null;

    /**
     * main             Program's entry point
     * @param argv      parameters see definitions above
     */
    public static void main(String[] argv)
    {
        Main.main = new Main();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        main.numberArgs = argv.length;
        System.out.println("broker: "+ main.broker);
        MQTTListener mqttListener = new MQTTListener(main.topic,main.clientId, main.broker, main.qos, main.user, main.password);
        mqttListener.setApi(main.api);
        mqttListener.setEcho(main.echo);
        if (main.api)
        {
            main.dbRestAPI = new DBRestAPI(null); // default
            mqttListener.setAPIDB(main.dbRestAPI);
        }
	    mqttListener.start();
        //demo code for reading database
        main.dbRestAPI.printDBResourceForPeriod("whole_house/voltage","2017-09-05T20:00:00.147Z", "2017-09-05T20:54:20.147Z");
        TimestampedDouble[] results;
        results = main.dbRestAPI.getDBResourceForPeriod("whole_house/voltage","2017-09-05T20:00:00.147", "2017-09-05T20:54:20.147");
        for (int i= 0; i < results.length; i++)
            System.out.println(results[i].toString());
    }

    /**
     *
     * @param name                  The name of the parameter to be validated
     * @param value                 Parameter value
     * @throws ParameterException   Thrown if the parameter is invalid (aborts program)
     */
    @Override
    public void validate(String name, String value) throws ParameterException
    {
        int n = Integer.parseInt(value);
        if (name.contains("q"))
        {
            if ((n < 0) | (n > 2)) // Check QOS is in range
            {
                throw new ParameterException("Parameter " + name + "should be  0,1 or 2" + "it was " + value + ")");
            }
        }
    }
}
