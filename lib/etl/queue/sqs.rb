require 'aws-sdk'
require 'etl/core'
require 'etl/queue/payload'

module ETL::Queue

  # Class that handles queueing using sqs for RabbitMQ
  class SQS < Base
    def initialize(params = {})
      if !params.key?(:url)
        # merge in the configuration.
        config = ::ETL.config.sqs
        params[:url] = config[:url]
        params[:region] = config[:region]
      end

      idle_timeout = params.fetch(:idle_timeout, nil)
      @queue_url = params.fetch(:url)
      @region = params.fetch(:region)

      @client = Aws::SQS::Client.new(region: @region)
      @poller = Aws::SQS::QueuePoller.new(@queue_url, { client: @client, idle_timeout: idle_timeout })
      @queue = Aws::SQS::Queue.new(url: @queue_url, client: @client)

      # Receive the message in the queue.
      @receive_message_result = @queue.receive_messages({
        message_attribute_names: ["All"], # Receive all custom attributes.
        max_number_of_messages: 1, # Receive at most one message.
        wait_time_seconds: 0 # Do not wait to check for the message.
      })
    end

    def to_s
      "#{self.class.name}: #{@queue_url}"
    end

    def enqueue(payload)
      resp = @queue.send_message({
        message_body: payload.encode,
      })
    end

    # Removes all jobs from the queue
    def purge
      @queue.purge
    end

    def message_count
      resp = @client.get_queue_attributes(queue_url: @queue_url, attribute_names: ["ApproximateNumberOfMessages"])
      resp.attributes["ApproximateNumberOfMessages"].to_i
    end

    def process_async
      @poller.poll(skip_delete: true) do |message|
        payload = ETL::Queue::Payload.decode(message.body)
        yield message, payload
      end
    end

    def ack(message)
      @queue.delete_messages({
        entries: [
          {
            id: message.message_id,
            receipt_handle: message.receipt_handle,
          },
        ],
      })
    end

    # Sqs seems to have slightly different semantics
    # we are acking after we have recieved the message but before 
    # we run it. Previously a job would be run on multiple workers
    # this is an attempt to stop that.
    def handle_incoming_messages
      process_async do |message_info, payload|
        begin
          log.debug("Payload: #{payload.to_s}")
        rescue StandardError => ex
          # Log and ignore all exceptions. We want other jobs in the queue
          # to still process even though this one is skipped.
          log.exception(ex)
        ensure
          # Acknowledge that this job was handled so we don't keep retrying and 
          # failing, thus blocking the whole queue.
          ETL.queue.ack(message_info)
        end
        begin
          ETL::Job::Exec.new(payload).run
        rescue StandardError => ex
          # Log and ignore all exceptions. We want other jobs in the queue
          # to still process even though this one is skipped.
          log.exception(ex)
        end

        # Just sleep indefinitely so the program doesn't end. This doesn't pause the
        # above block.
        while true
          sleep(10)
        end
      end
    end
  end
end

