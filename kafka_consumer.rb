require 'java'
require 'jbundler'

import 'java.util.Properties'
import 'java.util.UUID'
import 'org.apache.kafka.clients.consumer.KafkaConsumer'
import 'org.apache.kafka.clients.consumer.ConsumerRecord'
import 'org.apache.kafka.clients.consumer.ConsumerRecords'
import 'org.apache.kafka.common.serialization.StringDeserializer'

module RubyKafkaErrors
  def UnableToReadProperties < StandardError; end
end

class RubyKafkaConsumer
  def initialize(topic, properties, group_id = UUID.randomUUID.to_s)
    if properties.is_a? Hash
      @prop = properties_from_hash(properties)
    elsif properties.is_a? String
      @prop = properties_from_file(properties) if properties.is_a? String
    else
      raise(RubyKafkaErrors::UnableToReadProperties, 'Properties must be either a hash or a string path to an xml properties file')
    end
    @prop.put("group.id", group_id)
    @consumer = KafkaConsumer.new @prop
    @consumer.subscribe [topic]
  end

  def consume(timeout = 30, wait_for_new = 5)
    time_taken = 0
    while time_taken < timeout
      records = @consumer.poll(100)
      records.each do |record|
        puts "offset: #{record.offset} | key: #{record.key} | value: #{record.value}"
      end
      break if records.empty? && time_taken > wait_for_new
      sleep 1
      time_taken += 1
    end
    @consumer.close
  end

  # For debugging purposes only
  def properties_to_file(file_name)
    f = java.io.FileOutputStream.new(file_name, false)
    @prop.store_to_xml(f, 'Saved from kafka consumer')
  end

  private

  def properties_from_file(file_name)
    raise "Not Yet Implemented"
    java.io.FileInputStream.new(file_name)
    @prop = Properties.new
    @prop.load_from_xml
  end

  def properties_from_hash(prop_hash)
    props = Properties.new
    prop_hash.each do |key, value|
      props.put(key.to_s, value)
    end
    props
  end
end

if __FILE__ == $PROGRAM_NAME
  properties = { 'bootstrap.servers': 'localhost:9092',
                 'enable.auto.commit': 'true',
                 'auto.commit.interval.ms': '1000',
                 'auto.offset.reset': 'earliest',
                 'session.timeout.ms': '30000',
                 'key.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
                 'value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer' }
  properties_file = my_properties.xml
  consumer = RubyKafkaConsumer.new 'myTopic', properties
  consumer.properties_to_file 'my_properties.xml'
  consumer.consume
end
