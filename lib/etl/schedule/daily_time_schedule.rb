require 'models/job_run_repository'
require_relative 'base.rb'

module ETL::Schedule
  # Base class for job schedules. This object is given a job ID + batch and
  # is responsible for determining if we should be queueing that job for
  # running.
  class DailyTimes < Base
    attr_accessor :now_generator # added for testability
    attr_reader :times

    def initialize(times_to_run, job, batch, window_seconds = 65)
      super(job, batch)
      @times = []
      times_to_run.map do |v|
        @times << Time.parse(v) if v.is_a?(String)
        @times << v if v.is_a?(Time)
      end
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
      @times.each do |start_window|
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

  class DailyTimesByInterval < DailyTimes
    def initialize(start_time, interval_mins, job, batch, window_seconds = 65)
      current_time = Time.parse(start_time)
      times = []

      start_day = current_time.day
      while current_time.day == start_day
        times << current_time
        current_time += interval_mins * 60
      end
      puts # {
      super(times, job, batch, window_seconds)
    end
  end
end
