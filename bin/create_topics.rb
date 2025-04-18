require 'kafka'

kafka = Kafka.new(["Localhost:9092"], client_id:"topic-creator")

#first topic for stock prices
kafka.create_topic(
  "stock-prices",
  num_partitions: 3,
  replication_factor: 1
)

#second topic for stock news
kafka.create_topic(
  "stock-news",
  num_partitions: 2,
  replication_factor: 1
)

puts "topics created"