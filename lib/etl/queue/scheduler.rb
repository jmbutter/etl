module ETL::Queue

  # Class that handles adding jobs to the queue for processing by workers.
  # Checks to see what is currently queued and the desired frequency of jobs
  # to figure out what needs to be scheduled to run.
  class Scheduler
    def initialize(queue)
      @queue = queue
    end
    
    # Starts the infinite loop that processes jobs from the queue
    def start
      log.info("Starting job scheduler")
      while true      
        orgs = SchemaConfig.get_orgs(@params[:source], false) # only active orgs
        orgs << "" # Master db
        day = day_str()
        
        # sync the schema on each iteration
        sync_schema(@params[:source], @params[:dest])
        
        log.info("Starting iteration for day '#{day}' with #{orgs.count} orgs")
        jobs = 0
        orgs.each do |org|
          if org.empty?
            tables = SchemaConfig.get_master_source_tables(@params[:source])
          else
            tables = SchemaConfig.get_tenant_source_tables(@params[:source])
          end
                  
          log.info("Processing org '#{org}' with #{tables.count} tables")
          tables.each do |table|
            params = {
              source: @params[:source],
              dest: @params[:dest],
              org: org,
              table: table,
              day: day
            }
            log.info("Queued job: #{params}")
            enqueue(params)
            jobs += 1
          end
        end
        log.info("Queued #{jobs} jobs")

        # sleep until next iter
        wait_time = @params[:delay_mins]
        log.info("Sleeping for #{wait_time} minutes")
        sleep(wait_time * 60)
      end
      log.info("Job runner scheduler")
    end

    def log
      ETL.logger
    end
  end
end
