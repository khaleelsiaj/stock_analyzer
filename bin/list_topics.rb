require 'kafka'

kafka = Kafka.new(["localhost:9092"], client_id: "list-topics")

topics = kafka.topics

puts topics