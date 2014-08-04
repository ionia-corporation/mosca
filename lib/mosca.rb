require 'mqtt'
require 'json'

class Mosca
  @@default_broker = "test.mosquitto.org"
  @@default_timeout = 5
  @@debug = false

  attr_reader :options

  def initialize args = {}
    @options = default.merge(args)
  end

  def broker
    options[:broker]
  end

  def publish json, args = {}
    self.options = args
    connection do |c|
      c.subscribe(channel_in) if args[:response]
      c.publish(channel_out, json)
      get(options.merge({connection: c})) if args[:response]
    end
  end

  def get args = {}
    self.options = args
    connection do |c|
      Timeout.timeout(options[:timeout]) do
        c.get(channel_in) do |topic, message|
          parse_response message
          break
        end
      end
    rescue Timeout::Error
    end
  end

  def self.default_broker= broker
    @@default_broker = broker
  end

  def self.default_timeout= timeout
    @@default_timeout = timeout
  end

  private

    def options=(args)
      @options = @options.merge(args)
    end

    def default
      { topic_base: "",
        broker:     @@default_broker,
        client:     MQTT::Client }
    end

    def channel_out
      "#{ options[:topic_base] }#{ options[:topic_out] }"
    end

    def channel_in
      "#{ options[:topic_base] }#{ options[:topic_in] }"
    end

    def connection_options
      { remote_host: options[:broker],
        username:    options[:user],
        password:    options[:pass] }
    end

    def connection params = {}
      if params[:connection]
        yield params[:connection]
      else
        options[:client].connect(connection_options) do |c|
          yield c
        end
      end
    end

    def parse_response response
      if valid_json? response
        response = JSON.parse response
      end
      response
    end

    def valid_json? json
      begin
        JSON.parse(json)
        true
      rescue
        false
      end
    end

    def debug message
      puts message if @@debug
    end

    def timestamp
      Time.new.to_f.to_s
    end
end
