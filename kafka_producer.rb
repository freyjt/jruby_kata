require 'java'
require 'jbundler'

import 'java.util.Properties'
# do you need this import?
import 'org.apache.kafka.clients.producer.Producer'
import 'org.apache.kafka.clients.producer.KafkaProducer'
import 'org.apache.kafka.clients.producer.ProducerRecord'
import 'org.apache.kafka.common.serialization.StringSerializer'

class KafkaProducerRuby
  def initialize(topic)
    prop = Properties.new
    prop.put('bootstrap.servers', 'localhost:9092')
    prop.put('acks', 'all')
    prop.put('retries', 0.to_java(:int))
    prop.put('batch.size', 16384.to_java(:int))
    prop.put('linger.ms', 1.to_java(:int))
    prop.put('buffer.memory', 33554432.to_java(:int))
    prop.put('key.serializer', 'org.apache.kafka.common.serialization.StringSerializer')
    prop.put('value.serializer', 'org.apache.kafka.common.serialization.StringSerializer')
    @producer = KafkaProducer.new(prop)
    @topic = topic
  end

  def produce(key, value)
    record = ProducerRecord.new(@topic, key.to_s, value.to_s)
    @producer.send(record)
  end
end

if __FILE__ == $PROGRAM_NAME
  prod = KafkaProducerRuby.new('myTopic')
  60.times.each do |i|
    prod.produce(i.to_s, "Value is #{i.to_s}")
  end
  prod.close()
end
