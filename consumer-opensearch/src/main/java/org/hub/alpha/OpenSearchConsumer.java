package org.hub.alpha;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class OpenSearchConsumer {

    private final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

    private KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "opensearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

        return new KafkaConsumer<>(properties);
    }

    private RestHighLevelClient createOpenSearchClient() {
        String connString = "https://5gkjlxm7n5:jhx406g5ad@search-demo-3197097757.eu-central-1.bonsaisearch.net:443";
        URI connUri = URI.create(connString);
        String userInfo = connUri.getUserInfo();
        HttpHost openSearchHttpHost = new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme());
        RestHighLevelClient restHighLevelClient;

        if (userInfo == null) {
            RestClientBuilder restClientBuilder = RestClient.builder(openSearchHttpHost);
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            RestClientBuilder restClientBuilder = RestClient.builder(openSearchHttpHost)
                    .setHttpClientConfigCallback(
                            httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    );
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }

        return restHighLevelClient;
    }

    private String extractMessageId(String jsonMessage) {
        return JsonParser.parseString(jsonMessage)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) {
        OpenSearchConsumer openSearchConsumer = new OpenSearchConsumer();
        RestHighLevelClient openSearchClient = openSearchConsumer.createOpenSearchClient();
        KafkaConsumer<String, String> consumer = openSearchConsumer.createKafkaConsumer();

        try (openSearchClient; consumer) {
            String indexName = "wikimedia";
            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            boolean isIndexExisting = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!isIndexExisting) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                openSearchConsumer.logger.info("the wikimedia index has been created successfully");
            } else {
                openSearchConsumer.logger.info("the wikimedia index is already existing");
            }

            consumer.subscribe(List.of("wikimedia.recentchange"));

            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(9000));

                int recordsCount = records.count();
                openSearchConsumer.logger.info("received " + recordsCount + " record(s)");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    try {
//                        String recordID = record.topic() + "_" + record.partition() + "_" + record.offset();
                        String msgID = openSearchConsumer.extractMessageId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia");
                        indexRequest.source(record.value(), XContentType.JSON);
//                        indexRequest.id(recordID);
                        indexRequest.id(msgID);
                        bulkRequest.add(indexRequest);

//                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);\
//                        openSearchConsumer.logger.info("new record inserted with ID >> " + indexResponse.getId());
                    } catch (Exception e) {
                        openSearchConsumer.logger.error("error while parsing incoming message from Kafka Partition");
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    openSearchConsumer.logger.info("new bulk of records inserted with status >> " + bulkResponse.status());
                    openSearchConsumer.logger.info(bulkResponse.getItems().length + " record(s) inserted");

                    consumer.commitAsync((offsets, exception) -> openSearchConsumer.logger.info("offsets have been committed successfully!!"));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}