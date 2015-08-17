module ETL::Queue

  # Class that handles processing jobs from the queue as a background process
  class Worker
    def initialize(queue)
      @queue = queue
    end
    
    # Starts the infinite loop that processes jobs from the queue
    def start
      log.info("Starting job worker")
      @queue.process_async do |message_info, payload|
        begin
          log.debug(payload.to_s)
          ETL::Queue::Process.new(payload).run
        rescue StandardError => ex
          # Log and ignore all exceptions. We want other jobs in the queue
          # to still process even though this one is skipped.
          log.exception(ex)
        ensure
          # Acknowledge that this job was handled so we don't keep retrying and 
          # failing, thus blocking the whole queue.
          @queue.ack(message_info)
        end
      end

      # Just sleep indefinitely so the program doesn't end. This doesn't pause the
      # above block.
      while true
        sleep(10)
      end
      log.info("Job runner worker")
    end

    def log
      ETL.logger
    end
  end
end
