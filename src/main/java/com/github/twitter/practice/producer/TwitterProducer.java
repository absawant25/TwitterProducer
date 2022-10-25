package com.github.twitter.practice.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

  Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  String consumerKey = "J8suHncDvuTq3mvw2xZPOYqjV";
  String consumerSecret = "pv14fwochpfnN8CORwBuVn17C0UOdVX2B9jBZ7JrOZSYnunujo";
  String token = "48342519-P6uMLCQDHsyWM2iNJH9FM9fKUAj2546lwmCIdcB3m";
  String tokenSecret = "klW4lMj9iIGhr1OsMqbjKGMQMNfRbpVebRtOX2ErIMGfa";

  List<String> terms = Lists.newArrayList("kafka");

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {

    logger.info("Start of  application");

    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
    //create a twitter client
    Client client = createTwitterClient(msgQueue);
    // Attempts to establish a connection.
    client.connect();

    //create a kafka producer

    KafkaProducer<String, String>  producer= createKafkaProducer();


    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("stopping application...");
      logger.info("shutting down client from twitter..");
      client.stop();
      logger.info("closing producer...");
      producer.close();
    }));


    //loop to send tweets to kafka
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }

      if (msg != null) {
        logger.info(msg);
        producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null) {
              logger.error("Something bad happened", e);
            }
          }
        });
      }

    }

    logger.info("End of application");
  }

  public KafkaProducer<String, String> createKafkaProducer() {
    String bootStrapServers = "127.0.0.1:9092";

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    return producer;
  }

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

    ClientBuilder builder = new ClientBuilder().name(
            "Hosebird-Client-01")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }


}
