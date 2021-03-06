require 'etl/queue/payload'
require 'bunny'

module ETL::Queue

  # Class that handles queueing using Bunny gem for RabbitMQ
  class RabbitMQ < Base

    def initialize(params)
      @params = params
      amqp_uri = params[:amqp_uri]
      if amqp_uri.nil? then
        opts = {
          host: params[:host],
          port: params[:port],
          heartbeat: params[:heartbeat],
          vhost: params[:vhost],
          threaded: params.fetch(:threaded, true),
          username: params[:username],
          password: params[:password],
        }
        @conn = Bunny.new(nil, opts)
      else
        @conn = Bunny.new(amqp_uri,
          heartbeat: params[:heartbeat],
          vhost: params[:vhost],
          threaded: params.fetch(:threaded, true),
          )
      end

      @conn.start
      @channel = @conn.create_channel(nil, params[:channel_pool_size])
      @channel.prefetch(params[:prefetch_count])
      @queue = @channel.queue(params[:queue], :durable => true)
      @block = params.fetch(:block, true)
    end

    def to_s
      "#{self.class.name}<#{@params[:amqp_uri]}/#{@params[:vhost]}/#{@params[:queue]}>"
    end

    # Adds the passed in job details to the run queue
    # hash: Contains the following parameters needed to specify which job to run:
    # * source: Source database identifier
    # * dest: Destination database identifier
    # * org: Organization
    # * day: String in YYYY-MM-DD format representing the day
    # * table: Name of the table we're loading
    def enqueue(payload)
      @queue.publish(payload.encode, :persistent => true)
    end

    # Removes all jobs from the queue
    def purge
      @queue.purge
    end

    def message_count
      @queue.message_count
    end

    def process_async
      @queue.subscribe(:manual_ack => true, :block => @block) do |delivery_info, properties, body|
        payload = ETL::Queue::Payload.decode(body)
        yield delivery_info.delivery_tag, payload
      end
    end

    def ack(msg_info)
      @channel.ack(msg_info)
    end
  end
end
