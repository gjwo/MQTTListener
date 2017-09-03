package org.ladbury.mqqtListener;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
public class Main implements IParameterValidator
{
    private JCommander jc;
    private int numberArgs =0;
    private static Main main;
    @Parameter(names = {"--help", "-h",},description = "Display help information", help = true)
    private boolean help=false;
    @Parameter(names={"--topic","-t" }, description = "MQTT topic maintopic/subtopic/etc ")
    private String topic;
    @Parameter(names={"--client","-c" }, description = "The name of this MQTT client")
    private String clientId;
    @Parameter(names={"--broker","-b" },description = "The protocol, address and port of the broker, e.g.tcp://localhost:1883")
    private String broker;
    @Parameter(names={"--qos","-q" }, description = "0: The broker/client will deliver the message once, with no confirmation.\n"+
                                                    "1: The broker/client will deliver the message at least once, with confirmation required.\n"+
                                                    "2: The broker/client will deliver the message exactly once by using a four step handshake.")
    private int qos;

    public static void main(String[] argv)
    {
        Main.main = new Main();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        MQTTListener mqttListener = new MQTTListener(main.topic,main.clientId, main.broker, main.qos);
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
