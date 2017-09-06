package org.ladbury.mqqtListener;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.json.JSONArray;

import java.util.HashMap;

public class DBRestAPI
{
    public static final String DEFAULT_API_URL = "http://192.168.1.127:3000/api/";
    public static final int REST_REQUEST_SUCCESSFUL = 200;

    private final HashMap<String, WebResource> resources;
    private WebResource webResource;
    private final Client restClient;
    private int lastRestError;
    private ClientResponse clientResponse;
    private final String apiUrl;

    DBRestAPI(String apiUrl)
    {
        resources = new HashMap<>();
        webResource = null;
        clientResponse = null;
        restClient = Client.create();
        lastRestError = REST_REQUEST_SUCCESSFUL;
        if ((apiUrl == null) || (apiUrl.isEmpty()))
            this.apiUrl =  DEFAULT_API_URL;
        else this.apiUrl = apiUrl;
    }

    private synchronized WebResource getResource(String resource)
    {
        if(!resources.containsKey(resource))
            resources.put(resource, restClient.resource(this.apiUrl + resource));
        return resources.get(resource);
    }
    // Implementation of MqttCallback interface

    public int postToDB(String resource, String json)
    {
        webResource = getResource(resource);

        clientResponse = webResource.type("application/json")
                .post(ClientResponse.class, json);
        lastRestError = clientResponse.getStatus();

        return clientResponse.getStatus();
    }

    public void printDBResourceForPeriod(String resource, String start, String end)
    {

        clientResponse = getResource(resource)
                .queryParam("start", start)
                .queryParam("end", end)
                .get(ClientResponse.class);
        lastRestError = clientResponse.getStatus();
        if (lastRestError != REST_REQUEST_SUCCESSFUL) printLastError();
        else
        {
            JSONArray data = new JSONArray(clientResponse.getEntity(String.class));
            for (int i = 0; i < data.length(); i++)
            {
                System.out.println(data.getJSONObject(i).getDouble("reading") + " recorded at " +
                        data.getJSONObject(i).getString("timestamp"));
            }
        }
    }
    public TimestampedDouble[] getDBResourceForPeriod(String resource, String start, String end)
    {

        clientResponse = getResource(resource)
                .queryParam("start", start)
                .queryParam("end", end)
                .get(ClientResponse.class);
        lastRestError = clientResponse.getStatus();
        if (lastRestError != REST_REQUEST_SUCCESSFUL)
        {
            printLastError();
            return null;
        }
        else
        {
            JSONArray data = new JSONArray(clientResponse.getEntity(String.class));
            TimestampedDouble[] results = new TimestampedDouble[data.length()];
            for (int i = 0; i < data.length(); i++)
            {
                results[i] = new TimestampedDouble(data.getJSONObject(i).getDouble("reading"),
                        data.getJSONObject(i).getString("timestamp"));
            }
            return results;
        }
    }
    public void printLastError()
    {
        if (lastRestError == REST_REQUEST_SUCCESSFUL)
        {
            System.out.println("REST request was successful");
        }
        {
            System.out.println("Failed : HTTP error code : "
                    + clientResponse.getStatus() + " "
                    + clientResponse.getEntity(String.class));
        }
    }
}
