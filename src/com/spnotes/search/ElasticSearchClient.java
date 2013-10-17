package com.spnotes.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Map;

/**
 * Created by gpzpati on 10/16/13.
 */
public class ElasticSearchClient {

    private static final String HOST_NAME ="localhost";
    private static final int PORT = 9300;

    public static void main(String[] argv){

    }


    private static Client client;
    private Client getClient(){
        if(client == null){
            Settings settings = ImmutableSettings
                    .settingsBuilder()
                    .build();
            client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(HOST_NAME,PORT));

        }
        return client;
    }

    public Map getRecordById(String index, String type, String id){
        Client client = getClient();
        GetResponse getResponse = client.prepareGet(index, type, id).execute().actionGet();
        return getResponse.getSource();
    }

    public void insertRecord(String index, String type, Map sourceMap){
        Client client = getClient();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex(index, type).setSource(sourceMap));
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    }

    public void deleteRecord(String index, String type, String id){

    }
}
