package com.onlineshoppingsolutions.elasticsearch.index;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.shield.ShieldPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class LoaderApplication {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderApplication.class);
    private String clusterId;
    private String region;
    private String username;
    private String password;
    private int numberOfShards;
    private int recordCount;
    private int batchSize;
    private String currentAddress;

    public LoaderApplication(String clusterId, String region, String username, String password, int numberOfShards,
                             int recordCount, int batchSize) {
        this.clusterId = clusterId;
        this.region = region;
        this.username = username;
        this.password = password;
        this.numberOfShards = numberOfShards;
        this.recordCount = recordCount;
        this.batchSize = batchSize;
    }

    public void updateIndex() {

        String aliasName = "example_index";
        String indexNamePrefix = aliasName + "_";

        String currentIndexName = getCurrentIndexName(indexNamePrefix);
        Client client = null;

        try {
            client = getClient();
            // Create the index and mapping if it does not exist
            if (!isIndexCreated(client, currentIndexName)) {
                createIndexMapping(client, currentIndexName);
                indexMockJsonData(currentIndexName, client);
            }
        } catch (UnknownHostException e) {
            LOG.error("Problem connecting to elastic search", e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private void indexMockJsonData(String currentIndexName, Client client) {
        // wait for cluster index to be healthy before indexing
        LOG.info("Waiting for index health to be green");
        client.admin().cluster().prepareHealth(currentIndexName).setWaitForGreenStatus().get();

        BulkProcessor bulkProcessor = getBulkProcessor(client);
        final String[] SYSTEM_SETTING = new String[]{"SETTING_1",
                "SETTING_2",
                "SETTING_3",
                "SETTING_4",
                "SETTING_5",
                "SETTING_6"};

        final Random RAND = new Random();

        for (int i = 0; i < recordCount; i++) {
            addRandomRecordToBulk(currentIndexName, bulkProcessor, SYSTEM_SETTING, RAND);
        }

        try {
            bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOG.error("Problem waiting for batch to process");
        }
    }

    private void addRandomRecordToBulk(String currentIndexName, BulkProcessor bulkProcessor, String[] SYSTEM_SETTING, Random RAND) {
        try {

            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("user_id", RAND.nextInt(Integer.MAX_VALUE))
                    .field("another_id", RAND.nextInt(1000));

            int numSettings = RAND.nextInt(SYSTEM_SETTING.length);
            if (numSettings == 0) {
                numSettings = 1;
            }
            for (int j = 0; j < numSettings; j++) {
                builder = builder.field(SYSTEM_SETTING[j], RAND.nextBoolean() ? 1 : 0);
            }
            builder = builder.endObject();

            bulkProcessor.add(new IndexRequest(currentIndexName, "system_setting").source(builder));

        } catch (IOException e) {
            LOG.error("Problem building example JSON", e);
        } catch(Exception e){
            LOG.error("Problem indexing for address: " + currentAddress);
        }
    }

    private BulkProcessor getBulkProcessor(Client client) {
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        LOG.debug("Sending: " + request.numberOfActions() + " records, in execution: " + executionId + ", to: " + currentAddress);
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        if(response.hasFailures()){
                            LOG.error(response.buildFailureMessage());
                        }else{
                            LOG.info("Successful execution request: " + executionId + ", address: " + currentAddress);
                        }
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        LOG.info("Bulk Load Failed : " + executionId + ", address: " + currentAddress + ", message: " + failure.getMessage());
                    }
                })
                .setBulkActions(batchSize)
                .setBulkSize(new ByteSizeValue(8, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(0)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(4000), 3))
                .build();
        return bulkProcessor;
    }


    private void createIndexMapping(Client client, String currentIndexName) {

        // wait for cluster index to be healthy before indexing
        client.admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .get();

        // initially create index with zero replicas to improve indexing performance, create replicas after bulk load
        client.admin().indices().prepareCreate(currentIndexName).setSettings(Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", -1)).get();
    }

    private  boolean isIndexCreated(Client client, String currentIndexName) {
        return client.admin().indices()
                .prepareExists(currentIndexName)
                .execute().actionGet().isExists();
    }

    private String getCurrentIndexName(String indexPrefix) {
        ZonedDateTime utc = ZonedDateTime.now(ZoneOffset.UTC);
        DateTimeFormatter indexTimeStampFormatter = DateTimeFormatter.ofPattern("YYYYMMddHHmmss");
        String indexTimeStamp = indexTimeStampFormatter.format(utc);
        return indexPrefix + indexTimeStamp;
    }

    private  Client getClient() throws UnknownHostException {
        // Build the settings for our client.
        boolean enableSsl = true;
        Settings settings = Settings.settingsBuilder()
                .put("transport.ping_schedule", "5s")
                //.put("transport.sniff", false)
                .put("cluster.name", clusterId)
                .put("action.bulk.compress", false)
                .put("shield.transport.ssl", enableSsl)
                .put("request.headers.X-Found-Cluster", clusterId)
                .put("shield.user", username + ":" + password)
                .build();

        String hostname = clusterId + "." + region + ".aws.found.io";

        InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(InetAddress.getByName(hostname), 9343);
        LOG.info("Connecting client to: " + inetSocketTransportAddress.getAddress());
        currentAddress = inetSocketTransportAddress.getAddress();
// Instantiate a TransportClient and add the cluster to the list of addresses to connect to.
// Only port 9343 (SSL-encrypted) is currently supported.
        Client client = TransportClient.builder()
                .addPlugin(ShieldPlugin.class)
                .settings(settings)
                .build()
                .addTransportAddress(inetSocketTransportAddress);
        return client;

    }

    public static void main(String[] args) {
        java.security.Security.setProperty("networkaddress.cache.ttl" , "5");
        LoaderApplication loaderApplication = null;
        if(args.length > 5){
            loaderApplication = new LoaderApplication(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
        }else{
            System.out.println("java -jar elasticsearch-indexer-all-1.0-SNAPSHOT.jar <clusterId> <region> <username> <password> 3 10000 1000");
        }

        loaderApplication.updateIndex();
    }
}

