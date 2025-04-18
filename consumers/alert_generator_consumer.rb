require 'kafka'
require 'json'

class AlertGeneratorConsumer

  PRICE_THRESHOLD = 150
  def initialize 
    @kafka = Kafka.new(["localhost:9092"], client_id:"alert-consumer")
    @consumer = @kafka.consumer(group_id: "alert-group")
    @consumer.subscribe("stock-prices")
    @producer = @kafka.producer
  end

  def consume
    puts "Starting to read stock prices..."
    @consumer.each_message do |message| 
      message = JSON.parse(message.value)
      if message['price'] < PRICE_THRESHOLD 
        alert = {
          symbol: message['symbol'],
          price: message['price'],
          timestamp: Time.now
        }
        @producer.produce(
          alert.to_json,
          topic: "stock-alerts"
        )
        @producer.deliver_messages
        
        puts "Generated Alert for #{message['symbol']}: Price: #{message['price']} below #{PRICE_THRESHOLD}"
      end
    end 
  end
  def shutdown
    @consumer.stop
    @producer.shutdown
  end

end

if __FILE__ == $PROGRAM_NAME
  alert = AlertGeneratorConsumer.new
  begin
    alert.consume
  ensure
    alert.shutdown
  end
end

