require 'kafka'
require 'json'

class AlertsConsumer
  def initialize
    @kafka = Kafka.new(["localhost:9092"], client_id: "alert-consumer")
    @consumer = @kafka.consumer(group_id: "alert-log")
    @consumer.subscribe("stock-alerts")
  end
  

  def consume
    puts "currently reading kafka messages..."
    File.open("logs/alerts.log", "a") do |file|
      @consumer.each_message do |message|
        stock = JSON.parse(message.value)
        file.puts "#{stock['timestamp']}: Stock #{stock['symbol']} current price: #{stock['price']}"
        file.flush
        puts "#{stock['timestamp']}: Stock #{stock['symbol']} current price: #{stock['price']}"
      end
    end
  end
  
  def shutdown
    @consumer.stop
  end
  
end

if __FILE__ == $PROGRAM_NAME
  alert = AlertsConsumer.new
  begin
    alert.consume
  ensure
    alert.shutdown
  end
end

