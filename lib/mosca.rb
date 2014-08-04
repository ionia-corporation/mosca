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
      debug "[start publish] #{ timestamp }"
      c.subscribe(subscribe_channel) if args[:response]
      c.publish(channel_out, json)
      debug "[end publish] #{ timestamp }"
      get(options.merge({connection: c})) if args[:response]
    end
  end

  def get args = {}
    self.options = args
    response = {}
    connection(args) do |c|
      begin
        Timeout.timeout(options[:timeout]) do
          debug "[start get] " + timestamp
          c.get(channel_in) do |topic, message|
            response = parse_response message
            break
          end
          debug "[end get] " + timestamp
        end
      rescue
      end
    end
    response
  end

  def self.default_broker= broker
    @@default_broker = broker
  end

  def self.default_timeout= timeout
    @@default_timeout = timeout
  end

  def self.debug= debug
    @@debug = debug
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

    def subscribe_channel
      "#{ options[:topic_base] }#{ options[:topic_in] }"
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
