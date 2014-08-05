require 'mqtt'
require 'json'

class Mosca
  @@default_broker = "test.mosquitto.org"
  @@default_timeout = 5

  attr_accessor :options

  attr_reader :temporal_options

  def initialize args = {}
    @options = default.merge(args)
  end

  def broker
    options[:broker]
  end

  def publish json, args = {}
    self.temporal_options = args
    connection do |c|
      c.subscribe(subscribe_channel) if args[:response]
      c.publish(channel_out, json)
      get(connection: c) if args[:response]
    end
  end

  def get args = {}
    self.temporal_options = args
    response = {}
    connection do |c|
      begin
        Timeout.timeout(timeout) do
          c.get(channel_in) do |topic, message|
            response = parse_response message
            break
          end
        end
      rescue Timeout::Error
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

  private

    def temporal_options= args
      @temporal_options = @options.merge(args)
    end

    def default
      { topic_base: "",
        broker:     @@default_broker,
        client:     MQTT::Client }
    end

    def timeout
      temporal_options[:timeout]
    end

    def channel_out
      "#{ temporal_options[:topic_base] }#{ temporal_options[:topic_out] }"
    end

    def channel_in
      "#{ temporal_options[:topic_base] }#{ temporal_options[:topic_in] }"
    end

    def connection_options
      { remote_host: temporal_options[:broker],
        username:    temporal_options[:user],
        password:    temporal_options[:pass] }
    end

    def connection
      if temporal_options[:connection]
        yield temporal_options[:connection]
      else
        temporal_options[:client].connect(connection_options) do |c|
          yield c
        end
      end
    end

    def parse_response response
      JSON.parse response
      rescue JSON::ParserError
        response
    end

    def timestamp
      Time.new.to_f.to_s
    end
end
