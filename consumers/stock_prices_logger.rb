require 'json'
require 'kafka'


class LoggingConsumer
  def initialize
    @kafka = Kafka.new(["localhost:9092"], client_id: "stock-consumer")
    @consumer = @kafka.consumer(group_id: "logging-group")
    @consumer.subscribe("stock-prices")
  end

  def consume
    puts "Currently reading kafka messages..."
    File.open("logs/stock_prices.log", "a") do |file|
      @consumer.each_message do |message|
        stock = JSON.parse(message.value)
        file.puts("#{Time.now} - Stock #{stock['symbol']}: #{stock['price']}$")
        file.flush
      end
    end
  end
end
    
LoggingConsumer.new.consume