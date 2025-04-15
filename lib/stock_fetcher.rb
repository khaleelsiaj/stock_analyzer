require 'bundler/setup'
require 'json'
require 'kafka'
require 'httparty'
require_relative 'stock'

class StockFetcher

  BASE_URL = 'https://www.alphavantage.co/query?function=GLOBAL_QUOTE'
  Api_key = "OT67LXQ2DNO66J6V"

  def initialize
    @kafka = Kafka.new(["localhost:9092"], client_id: "stock-fetcher")
  end

  def fetch(symbol)
    url = "#{BASE_URL}&symbol=#{symbol}&apikey=#{Api_key}"
    respons = HTTParty.get(url)
    data = respons.parsed_response["Global Quote"]

    #check if the data is correct
    if data && data["05. price"]
      stock = Stock.new(symbol, data["05. price"])

      #send to kafka
      @kafka.producer.produce(stock.to_json, topic: "stock-prices")
      @kafka.producer.deliver_messages
      puts "sent #{stock.to_json} to kafka"

    else
      puts "Couldn't fetch data for sybmol #{symbol}"
    end
  end
end



