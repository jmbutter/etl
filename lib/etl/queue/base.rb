require 'etl/queue/payload.rb'
require 'etl/job/exec'
require 'etl/mixins/cached_logger'

module ETL::Queue

  # Base class that defines the interface our work queues need
  class Base
    include ETL::CachedLogger
    # Starts async processing of the queue. When an element is read off the 
    # queue the |message_info, payload| is passed to block.
    def process_async(&block)
    end
    
    # Places the specified payload onto the queue for processing by a worker.
    def enqueue(payload)
    end
    
    # Purges all jobs from the queue
    def purge
    end

    # Returns number of messages in the queue
    def message_count
      0
    end
    
    # Acknowledges that the specified message d
    def ack(msg_info)
    end

    def handle_incoming_messages
      process_async do |message_info, payload|
        begin
          log.debug("Payload: #{payload.to_s}")
          ETL::Job::Exec.new(payload).run
        rescue StandardError => ex
          # Log and ignore all exceptions. We want other jobs in the queue
          # to still process even though this one is skipped.
          log.exception(ex)
        ensure
          # Acknowledge that this job was handled so we don't keep retrying and 
          # failing, thus blocking the whole queue.
          ETL.queue.ack(message_info)
        end
      end

      # Just sleep indefinitely so the program doesn't end. This doesn't pause the
      # above block.
      while true
        sleep(10)
      end
    end
  end
end
