require 'json'
require 'kafka'
require 'httparty'

class StockPriceFetcher
  BASE_URL = 'https://www.alphavantage.co/query?function=GLOBAL_QUOTE'
  Api_key = "QOVPR5RUU8TBD4EW"

  def initialize
    @kafka = Kafka.new(["localhost:9092"], client_id: "stock-fetcher")
    @producer = @kafka.producer
  end

  def fetch(symbol)
    begin
      url = "#{BASE_URL}&symbol=#{symbol}&apikey=#{Api_key}"
      response = HTTParty.get(url)
      puts response.body
      data = response.parsed_response["Global Quote"]
    rescue StandardError => e
      puts(e.message)
    end

    #check if the data is correct
    if data && data["05. price"]
      price = data["05. price"].to_f
      message = {
        symbol: symbol,
        price: price
    }.to_json
      @producer.produce(message, topic: "stock-prices")
      @producer.deliver_messages
      puts "sent #{message} to kafka"
    else
      puts "Couldn't fetch data for sybmol #{symbol}"
    end
  end

  def shutdown
    @producer.shutdown
  end
end


if __FILE__ == $PROGRAM_NAME
  producer = StockPriceFetcher.new
  begin
    producer.fetch("INTC")
  ensure
    producer.shutdown
  end
end

