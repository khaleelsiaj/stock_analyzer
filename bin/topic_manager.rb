require 'kafka'

class TopicManager
  def initialize(brokers, client_id)
    @kafka = Kafka.new(brokers, client_id: client_id)
  end

  def create_topic(name, num_partitions: 1, replication_factor: 1)
    begin
      @kafka.create_topic(
        name,
        num_partitions: num_partitions,
        replication_factor: replication_factor
      )
      puts "Topic '#{name}' created successfully."
    rescue Kafka::TopicAlreadyExists
      puts "Topic '#{name}' already exists."
    rescue => e
      puts "Failed to create topic '#{name}': #{e.message}"
    end
  end

  def list_topics
    puts "Available topics:"
    @kafka.topics.each { |topic| puts "- #{topic}" }
  end
end

# If running directly
if __FILE__ == $PROGRAM_NAME
  manager = TopicManager.new(["localhost:9092"], "topic-manager")
  manager.create_topic("stock-prices", num_partitions: 3, replication_factor: 1)
  manager.create_topic("stock-news", num_partitions: 2, replication_factor: 1)
  manager.create_topic("stock-alerts", num_partitions: 1, replication_factor: 1)
  manager.list_topics
end
