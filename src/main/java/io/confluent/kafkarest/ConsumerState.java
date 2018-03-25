/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.confluent.kafkarest.entities.TopicPartitionOffset;
import jersey.repackaged.com.google.common.collect.ImmutableList;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

/**
 * Tracks all the state for a consumer. This class is abstract in order to support multiple
 * serialization formats. Implementations must provide decoders and a method to convert Kafka
 * MessageAndMetadata<K,V> values to ConsumerRecords that can be returned to the client (including
 * translation if the decoded Kafka consumer type and ConsumerRecord types differ).
 */
public abstract class ConsumerState<KafkaK, KafkaV, ClientK, ClientV>
    implements Comparable<ConsumerState> {

  private KafkaRestConfig config;
  private ConsumerInstanceId instanceId;
  private ConsumerConnector consumer;
  private final Object consumerLock = new Object();
  //The underlying consumer only allows for registering for a topic once and this code
  //only lets you pass one topic so we only need to track one topicState
  private String topic;
  private ConsumerTopicState topicState;
  private long expiration;
  // A read/write lock on the ConsumerState allows concurrent readTopic calls, but allows
  // commitOffsets to safely lock the entire state in order to get correct information about all
  // the topic/stream's current offset state. All operations on individual TopicStates must be
  // synchronized at that level as well (so, e.g., readTopic may modify a single TopicState, but
  // only needs read access to the ConsumerState).
  //private ReadWriteLock lock;

  public ConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
                       ConsumerConnector consumer) {
    this.config = config;
    this.instanceId = instanceId;
    this.consumer = consumer;
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public ConsumerInstanceId getId() {
    return instanceId;
  }

  /**
   * Gets the key decoder for the Kafka consumer.
   */
  protected abstract Decoder<KafkaK> getKeyDecoder();

  /**
   * Gets the value decoder for the Kafka consumer.
   */
  protected abstract Decoder<KafkaV> getValueDecoder();

  /**
   * Converts a MessageAndMetadata using the Kafka decoder types into a ConsumerRecord using the
   * client's requested types. While doing so, computes the approximate size of the message in
   * bytes, which is used to track the approximate total payload size for consumer read responses to
   * determine when to trigger the response.
   */
  public abstract ConsumerRecordAndSize<ClientK, ClientV> createConsumerRecord(
      MessageAndMetadata<KafkaK, KafkaV> msg);

  /**
   * Start a read on the given topic, enabling a read lock on this ConsumerState and a full lock on
   * the ConsumerTopicState.
   */
  public void startRead(ConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> topicState) {
    lock.readLock().lock();
    topicState.lock();
  }

  /**
   * Finish a read request, releasing the lock on the ConsumerTopicState and the read lock on this
   * ConsumerState.
   */
  public void finishRead(ConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> topicState) {
    topicState.unlock();
    lock.readLock().unlock();
  }

  public List<TopicPartitionOffset> commitOffsets() {

    ImmutableList.Builder<TopicPartitionOffset> ret = ImmutableList.builder();
    ImmutableMap<Integer, Long> stateOffsets = topicState.commitOffsets();
    for (Map.Entry<Integer, Long> offset : stateOffsets.entrySet())
    {
      ret.add(new TopicPartitionOffset(topic, offset.getKey(),
            offset.getValue(), offset.getValue()));
    }

    return ret.build();
  }

  public void close() {
    synchronized (consumerLock)
    {
      topic = null;
      topicState = null;
      consumer.shutdown();
      // Marks this state entry as no longer valid because the consumer group is being destroyed.
      consumer = null;

    }
  }

  public boolean expired(long nowMs) {
    return expiration <= nowMs;
  }

  public void updateExpiration() {
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public long untilExpiration(long nowMs) {
    return this.expiration - nowMs;
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public void setConfig(KafkaRestConfig config) {
    this.config = config;
  }

  @Override
  public int compareTo(ConsumerState o) {
    if (this.expiration < o.expiration) {
      return -1;
    } else if (this.expiration == o.expiration) {
      return 0;
    } else {
      return 1;
    }
  }

  public ConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV> getOrCreateTopicState(String topic) {
    // Try getting the topic only using the read lock

    synchronized (consumerLock)
    {
      if (this.topic != null && topic.equals(this.topic))
      {
        throw Errors.consumerAlreadySubscribedException();
      }

      if (topicState != null)
      {
        return topicState;
      }

      if (consumer == null)
      {
        return null;  //we are shutting down
      }

      try
      {
        Map<String, Integer> subscriptions = new TreeMap<String, Integer>();
        subscriptions.put(topic, 1);
        Map<String, List<KafkaStream<KafkaK, KafkaV>>> streamsByTopic =
              consumer.createMessageStreams(subscriptions, getKeyDecoder(), getValueDecoder());
        KafkaStream<KafkaK, KafkaV> stream = streamsByTopic.get(topic).get(0);
        topicState = new ConsumerTopicState<KafkaK, KafkaV, ClientK, ClientV>(consumer, stream);
        this.topic = topic;
        return topicState;
      }
      catch (MessageStreamsExistException e)
      {
        throw Errors.consumerAlreadySubscribedException();
      }
    }
  }

}