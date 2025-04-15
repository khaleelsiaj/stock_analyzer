require 'json'

class Stock
  attr_accessor :symbol, :price

  def initialize(symbol, price)
    @symbol = symbol
    @price = price
  end
  
  def to_json(*args)
    {symbol: @symbol, price: @price }.to_json(*args)
  end
end

