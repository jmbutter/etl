module ETL::Queue

  # Class that processes payloads that come off of the job queue
  class Process
    attr_accessor :payload
    
    # Initialize with payload we received from the queue
    def initialize(payload)
      @payload = payload
    end

    # Run the job for this object's payload and handles any immediate retries.
    # Saves job result in the DB. Throws exception on error.
    def run
      # retry parameters: 4s, 8s, 16s, 32s, 64s
      retries = 0
      retry_max = 5
      retry_wait = 4
      retry_mult = 2.0
      
      # get batch and job model out of the payload
      batch, jm = extract_payload
      
      # instantiate the input class
      input = Object::const_get(jm.input_class).new(jm.input_params)
      
      # prepare the job to run
      job = Object::const_get(jm.job_class).new(jm.job_params)
      job.reader = input
      job.feed_name = jm.feed_name
      job.batch = batch
      
      # get a run for this job
      jr = ETL::Model::JobRun.find_or_create(jm, batch)
      
      # change status to running
      jr.running()
        
      begin
        result = job.run()
        jr.success(result)
      rescue Sequel::DatabaseError => ex
        # By default we want to retry database errors...
        do_retry = true
        
        # But there are some that we know are fatal
        do_retry = false if ex.message.include?("Mysql2::Error: Unknown database")
        
        # Help debug timeouts by logging the full exception
        if ex.message.include?("Mysql2::Error: Lock wait timeout exceeded")
          log.exception(ex, Logger::WARN)
        end
        
        # Retry this job with exponential backoff
        retries += 1
        do_retry &&= (retries <= retry_max)
        if do_retry
          log.warn("Database error '#{ex.message}' on attempt " +
            "#{retries}/#{retry_max}; waiting for #{retry_wait} seconds " +
            "before retrying")
          sleep(retry_wait)
          retry_wait *= retry_mult
          retry
        end
        
        # we aren't retrying anymore - mark job as failed and rethrow
        jr.exception(ex)
        raise
      rescue Exception => ex
        # for all other exceptions: save the message and rethrow
        jr.exception(ex)
        raise
      end
      
      return jr
    end
    
    private

    def log
      ETL.logger
    end

    def extract_payload
      # Extract info from payload
      job_model = @payload.job_model
      raise "Invalid job_id in payload: '#{@payload.job_id}'" unless job_model
      [@payload.batch, job_model]
    end
  end
end
