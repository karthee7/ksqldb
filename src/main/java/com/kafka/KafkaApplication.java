package com.kafka;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;

public class KafkaApplication {

  public static String KSQLDB_SERVER_HOST = "localhost";
  public static int KSQLDB_SERVER_HOST_PORT = 8088;

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    ClientOptions options = ClientOptions.create()
        .setHost(KSQLDB_SERVER_HOST)
        .setPort(KSQLDB_SERVER_HOST_PORT);
    Client client = Client.create(options);
    List<StreamInfo> resultStream = client.listStreams().get();
    resultStream.stream().forEach(streamInfo-> {
    	System.out.println(streamInfo.toString());
    });
    
    List<TopicInfo> resultTopic = client.listTopics().get();
    resultTopic.stream().forEach(topicInfo-> {
    	System.out.println(topicInfo.toString());
    });
    List<TableInfo> resultTable = client.listTables().get();
    resultTable.stream().forEach(topicInfo-> {
    	System.out.println(topicInfo.toString());
    });
    
	/*
	 * KsqlObject row = new KsqlObject() .put("userid", "karthee") .put("country",
	 * "ph");
	 * 
	 * client.insertInto("USER_STREAM", row).get();
	 */
    Map<String, Object> properties = new HashMap<>();
    properties.put("ksql.streams.auto.offset.reset","earliest");
	List<Row> rows = client.executeQuery("select  * from USER_TABLE_LIST_MAT where userid='karthee';", properties).get();
	rows.stream().forEach(row2-> {
    	System.out.println(row2.toString());
    });
	
	List<Row> rowOffset = client.executeQuery("select  * from USER_TABLE_LATEST_OFFSET_MAT where userid='karthee';", properties).get();
	rowOffset.stream().forEach(row1-> {
    	System.out.println(row1.toString());
    });
    
    
    client.close();
  }
}