require_relative '../slack/notifier'
require 'socket'

module ETL::Job
  # Class that indicates a retry should occur
  class RetryError < StandardError
    attr_accessor :inner_error
    def initialize(inner_error, msg="A Retry error has occured")
      error_msg = "#{msg}: #{inner_error.message}"
      super(error_msg)
      @inner_error = inner_error
    end
  end

  # Class that runs jobs given a payload
  class Exec
    # Initialize with payload we received from the queue
    def initialize(payload, params = {})
      @payload = payload
      @params = {
        retry_max: 5,
        retry_wait: 4,
        retry_mult: 2.0,
      }.merge(params)
    end

    # Run the job for this object's payload and handles any immediate retries.
    # Saves job result in the DB and returns the ETL::Model::JobRun object
    # produced. If the job has an exception this will (a) store that in the
    # job messages; (b) log it; (c) swallow it.
    def run
      begin
        retries ||= 0
        retry_wait = @params[:retry_wait]

        # Collect metrics
        measurements = {}
        # get batch and job model out of the payload
        batch, job = extract_payload
        # get a run for this job

        jrr = ::ETL::Model::JobRunRepository.instance
        jr = jrr.create_for_job(job, batch)

        # change status to running
        jr.running()
        notifier = job.notifier
        unless notifier.nil?
          host_name = Socket.gethostname
          notifier.notify("Starts running on #{host_name}")
        end

        result = job.run()
        jr.success(result)
        if !notifier.nil?
          if jr.success?
            notifier.set_color("#36a64f")
            notifier.add_text_to_attachments("# Processed rows: #{result.rows_processed}")
          else
            notifier.set_color("#ff0000")
          end
        end

        measurements[:rows_processed] = result.rows_processed

      rescue RetryError => ex
        # By default we want to retry database errors...
        do_retry = true

        # Retry this job with exponential backoff
        retries += 1
        do_retry &&= (retries <= @params[:retry_max])
        if do_retry
          log.warn("RetryError '#{ex.message}' inner_error '#{ex.inner_error.message}' on attempt " +
            "#{retries}/#{@params[:retry_max]}; waiting for #{retry_wait} seconds " +
            "before retrying")
          sleep(retry_wait)
          retry_wait *= @params[:retry_mult]
          retry
        end

        # we aren't retrying anymore - log this error
        jr.exception(ex)
        notifier.add_field_to_attachments({ "title" => "Error message", "value" => ETL::Logger.create_exception_message(ex)}) unless notifier.nil?

      rescue Sequel::DatabaseError => ex
        # By default we want to retry database errors...
        do_retry = true

        # But there are some that we know are fatal
        do_retry = false if ex.message.include?("Mysql2::Error: Unknown database")

        # We dont want to retry when load error happens in loading data from s3 to redshift 
        do_retry = false if ex.message.include?("ODBC::Error: XX000 (30)") && ex.message.include?("[Amazon Redshift] (30)")

        # Help debug timeouts by logging the full exception
        if ex.message.include?("Mysql2::Error: Lock wait timeout exceeded")
          log.exception(ex, Logger::WARN)
        end

        # Retry this job with exponential backoff
        retries += 1
        do_retry &&= (retries <= @params[:retry_max])
        if do_retry
          log.warn("Database error '#{ex.message}' on attempt " +
            "#{retries}/#{@params[:retry_max]}; waiting for #{retry_wait} seconds " +
            "before retrying")
          sleep(retry_wait)
          retry_wait *= @params[:retry_mult]
          retry
        end

        # we aren't retrying anymore - log this error
        jr.exception(ex)
        notifier.add_field_to_attachments({ "title" => "Error message", "value" => ETL::Logger.create_exception_message(ex)}) unless notifier.nil?
      rescue StandardError => ex
        # for all other exceptions: save the message
        jr.exception(ex) unless jr.nil? # When a batch fails to validate it can be nil
        notifier.add_field_to_attachments({ "title" => "Error message", "value" => ETL::Logger.create_exception_message(ex)}) unless notifier.nil?
      end

      if !notifier.nil?
        notifier.add_text_to_attachments("Job duration: #{jr.ended_at - jr.started_at}")
        notifier.notify("#{@payload.job_id} summary")
      end

      metrics.point(
        measurements.merge(
          job_time_secs: (jr.ended_at - jr.started_at),
          retries: retries
        ),
        tags: {
          status: jr.status,
          job_id: jr.job_id
        },
        time: jr.ended_at,
        type: :timer
      )

      return jr
    end

    private

    def log_context
      {
        job: @payload.job_id,
        batch: @payload.batch_hash.to_s,
      }
    end

    def log
      @log ||= ETL.create_logger(log_context)
    end

    def metrics
      @metrics ||= ETL.create_metrics
    end

    def job_manager
      ETL::Job::Manager.instance
    end

    def self.create_job(job_id, klass, batch)
      job_manager = ::ETL::Job::Manager.instance

      # instantiate the job class, if a klass factory exists use that to create it.
      klass_factory = job_manager.get_class_factory(job_id)
      if !klass_factory.nil? then
        job_obj = klass_factory.create(job_id, batch)
      else
        job_obj = klass.new(batch)
      end
      raise ETL::JobError, "Failed to instantiate job class: '#{klass.name}'" unless job_obj
      job_obj
    end

    def extract_payload
      # Extract info from payload
      klass = job_manager.get_class(@payload.job_id)
      raise ETL::JobError, "Failed to find job ID '#{@payload.job_id}' in manager when extracting payload" unless klass

      # instantiate and validate our batch class
      bf = klass.batch_factory
      batch = bf.validate!(bf.from_hash(@payload.batch_hash))

      job_obj = Exec.create_job(@payload.job_id, klass, batch)
      [batch, job_obj]
    end

  end
end
