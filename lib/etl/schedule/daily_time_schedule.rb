require 'models/job_run_repository'
require_relative 'base.rb'

module ETL::Schedule

  # Base class for job schedules. This object is given a job ID + batch and
  # is responsible for determining if we should be queueing that job for
  # running.
  class DailyTimes < Base
    attr_accessor :now_generator # added for testability

    def initialize(times_to_run, window_seconds = 65, job, batch)
      super(job, batch)
      @times_to_run = times_to_run.map { |v| Time.parse(v) }
      @window_seconds = window_seconds
    end

    # Returns true if we should queue this job for executing given the rules
    # of this schedule object. Looking to run a job within the cron window.
    def ready?
      has_pending = ::ETL::Model::JobRunRepository.instance.has_pending?(@job, @batch)
      return false if has_pending
      last_ended = ::ETL::Model::JobRunRepository.instance.last_ended(@job, @batch)
      now = get_current_time

      # ensure that the last time the job was run was before now.
      @times_to_run.each do |start_window|
        end_window = start_window + @window_seconds
        return false if last_ended > start_window && last_ended < end_window

        return true if now > start_window && now < end_window
      end
      false
    end

    def get_current_time
      return @now_generator.now unless @now_generator.nil?
      Time.now
    end
  end
end
