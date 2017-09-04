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
    @Parameter(names={"--trace","-t" }, description = "if present output a trace message")
    private boolean trace = false;
    @Parameter(names={"--api","-a" }, description = "if present output the message to the api")
    private boolean api = false;

    public static void main(String[] argv)
    {
        Main.main = new Main();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        main.numberArgs = argv.length;
        MQTTListener mqttListener = new MQTTListener(main.topic,main.clientId, main.broker, main.qos, main.user, main.password);
        mqttListener.setApi(main.api);
        mqttListener.setTrace(main.trace);
	    mqttListener.start();
    }

    @Override
    public void validate(String name, String value) throws ParameterException
    {
        int n = Integer.parseInt(value);
        if (name.contains("q"))
        {
            if ((n < 0) | (n > 2))
            {
                throw new ParameterException("Parameter " + name + "should be  0,1 or 2" + "it was " + value + ")");
            }
        }
    }
}
