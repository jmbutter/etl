require 'etl/core'
require 'mixins/cached_logger'
require 'pg'
require 'erb'
require 'thread'

module ETL::Model
  class << self
    attr_accessor :jrr_mutex
  end
  # handles interacting with the data store.
  class JobRunRepository
    include ETL::CachedLogger
    attr_accessor :schema_name
    # allows configuration of another Repository
    class << self; attr_accessor :instance end
    @@instance = ETL::Model::JobRunRepository.new

    def initialize(conn_params = nil, schema_name= 'public', pending_job_expiration_days = 2)
      @conn_params = conn_params
      @schema_name = schema_name
      @conn_params = ETL.config.core[:database] if @conn_params.nil?
      @conn_params = prep_conn(@conn_params)
      ETL::Model.jrr_mutex = Mutex.new if ETL::Model.jrr_mutex.nil?
    end

    def prep_conn(conn_params)
      # Adding this to allow the current configuration files to work with pg lib
      # separate function so useful for tests.
      if conn_params[:adapter] == "postgres"
        conn_params.delete(:adapter)
        conn_params.delete(:encoding)
        conn_params.delete(:reconnect)
        conn_params.delete(:database)
        conn_params.delete(:pool)
      end
      conn_params
    end

    def conn
      raise ArgumentError, "conn_params was never set" unless !@conn_params.nil?
      @conn ||= PG.connect(@conn_params)
    end

    def self.create_table_sql(schema_name = 'public')
      <<~HEREDOC
        CREATE TABLE IF NOT EXISTS  #{schema_name}.job_runs
        (
            id SERIAL PRIMARY KEY,
            created_at timestamp without time zone NOT NULL,
            updated_at timestamp without time zone NOT NULL,
            job_id text COLLATE pg_catalog."default" NOT NULL,
            batch text COLLATE pg_catalog."default" NOT NULL,
            status text COLLATE pg_catalog."default" NOT NULL,
            queued_at timestamp without time zone,
            started_at timestamp without time zone,
            ended_at timestamp without time zone,
            rows_processed integer,
            message text COLLATE pg_catalog."default"
        )
        WITH (
            OIDS = FALSE
        )
        TABLESPACE pg_default;
      HEREDOC
    end

    def create_table
      exec_sql(self.class.create_table_sql)
    end

    # Creates JobRun object from Job and batch date
    def create_for_job(job, batch)
      jr = JobRun.new(self)
      jr.created_at = Time.now
      jr.job_id = job.id
      jr.status = :new
      jr.batch = batch.to_json
      insert_sql = "INSERT INTO #{@schema_name}.job_runs(created_at, updated_at, job_id, batch, status) VALUES ('#{Time.now}','#{Time.at(0)}', '#{job.id}', '#{batch.to_json}', 'new') RETURNING id";
      r = exec_sql(insert_sql).values
      if r.length == 0
        return nil
      end
      jr.id = r.first[0].to_i
      jr
    end

    def save(jr)
      update_sql = "UPDATE #{schema_name}.job_runs SET status='#{jr.status.to_s}', updated_at='#{jr.updated_at}'"
      if !jr.message.nil?
        escaped_message = "E'" + @conn.escape_string(jr.message).gsub("\n", "\\n")+"'"
        update_sql = update_sql + ", message=#{escaped_message}"
      end
      if !jr.ended_at.nil?
        update_sql = update_sql + ", ended_at='#{jr.ended_at}'"
      end
      if !jr.queued_at.nil?
        update_sql = update_sql + ", queued_at='#{jr.queued_at}'"
      end
      if !jr.started_at.nil?
        update_sql = update_sql + ", started_at='#{jr.started_at}'"
      end
      if !jr.rows_processed.nil?
        update_sql = update_sql + ", rows_processed=#{jr.rows_processed}"
      end

      update_sql = update_sql + " WHERE id = #{jr.id}"
      exec_sql(update_sql)
      jr
    end

    def tables
      sql = "SELECT tablename FROM pg_catalog.pg_tables where schemaname = '#{@schema_name}'"
      r = exec_sql(sql)
      table_names = []
      r.each do |row|
        table_names << row["tablename"]
      end
      table_names
    end

    def table_schema(table_name)
      sql = "SELECT * FROM information_schema.columns WHERE table_schema = '#{@schema_name}' AND table_name = '#{table_name}'"
      r = exec_sql(sql)
      columns = {}
      r.each do |c|
        columns[c["column_name"].to_st ] = c["data_type"]
      end
      columns
    end

    def delete_all
      sql = "DELETE from #{@schema_name}.job_runs;"
      job_run_query(sql)
    end

    def all
      sql = "Select * from #{@schema_name}.job_runs;"
      job_run_query(sql)
    end

    # various query methods below
    def find(job, batch)
      sql = "Select * from #{@schema_name}.job_runs where job_id = '#{job.id}' and batch = '#{batch.to_json}';"
      job_run_query(sql)
    end

    # Finds all "pending" runs for specified job and batch
    # Pending means the job is either queued or currently running
    def find_pending(job, batch)
      sql = "Select * from #{@schema_name}.job_runs where job_id = '#{job.id}' and batch = '#{batch.to_json}' and (status = 'queued' or status = 'running' );"
      job_run_query(sql)
    end

    # Finds all batches currently of a specified status starting from the time specified
    # If a job was aborted sometimes its not noted as completed in the database so
    # only looking at jobs from a certain time forward. Default is now - 1 hour
    # which can be re-configured
    def find_by_status(status, initial_start_time)
      initial_start_time = Time.now - 1 * 60 * 60 if initial_start_time.nil?
      sql = "Select * from #{@schema_name}.job_runs where status = '#{status}' and started_at > '#{initial_start_time}';"
      job_run_query(sql)
    end

    # find the last successful batch for the specified job
    # returns no values if none found.
    def find_latest_job_run(job_id, opts = {})
      status = opts[:status]
      group_by = 'group by id, created_at, updated_at, job_id, batch, status, queued_at, started_at, ended_at, rows_processed, message'
      sql = "select Max(ended_at) as ended_at, id, created_at, updated_at, job_id, batch, status, queued_at, started_at, rows_processed, message from #{@schema_name}.job_runs where job_id = '#{job_id}'"
      unless status.nil?
        sql = sql + " and status = '#{status}'"
      end
      sql = sql + " #{group_by}"
      job_run_query(sql)
    end

    # Returns true if this job+batch has pending jobs
    def has_pending?(job, batch)
      sql = "Select count(*) from #{@schema_name}.job_runs where job_id = '#{job.id}' and batch = '#{batch.to_json}' and ( status = 'queued' or status = 'running' );"
      r = exec_sql(sql)
      count = r.first["count"].to_i
      count >  0
    end

    # Returns whether there have been any successful runs of this job+batch
    def was_successful?(job, batch)
      sql = "Select * from #{@schema_name}.job_runs where job_id = '#{job.id}' and batch = '#{batch.to_json}' and status = 'success';"
      r = exec_sql(sql)
      r.cmd_tuples > 0
    end

    # Returns the last ended JobRun, or nil if none has ended
    # Note that a ended job can be either success or error
    def last_ended(job, batch)
      sql = "Select * from #{schema_name}.job_runs where job_id = '#{job.id}' and batch = '#{batch.to_json}' and (status = 'success' or status = 'error') ORDER BY ended_at DESC;"
      runs = job_run_query(sql)
      runs.first
    end

    def job_run_query(sql)
      r = exec_sql(sql)
      job_runs = []
      r.each do |single_result|
        job_runs << ::ETL::Model::JobRunRepository.build_job_run(self, single_result)
      end
      job_runs
    end

    def self.build_job_run(repository, r)
      jr = ETL::Model::JobRun.new(r)
      jr.id = r["id"].to_i
      jr.created_at = Time.parse(r["created_at"]) unless r["created_at"].nil?
      jr.updated_at = Time.parse(r["updated_at"])
      jr.job_id = r["job_id"]
      jr.batch = r["batch"]
      jr.status = r["status"].to_sym
      jr.queued_at = Time.parse(r["queued_at"]) unless r["queued_at"].nil?
      jr.started_at = Time.parse(r["started_at"]) unless r["started_at"].nil?
      jr.ended_at = Time.parse(r["ended_at"]) unless r["ended_at"].nil?
      jr.rows_processed = r["rows_processed"].to_i unless r["rows_processed"].nil?
      jr.message = r["message"]
      jr
    end

    private

      def exec_sql(sql)
        result = nil

        ETL::Model.jrr_mutex.synchronize do
          result = conn.exec(sql)
        end
        result
      end
  end
end
