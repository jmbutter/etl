require 'models/job_run_repository'
require_relative 'base.rb'

module ETL::Schedule
  # Base class for job schedules. This object is given a job ID + batch and
  # is responsible for determining if we should be queueing that job for
  # running.
  class DailyTimes < Base
    include ETL::CachedLogger

    attr_accessor :now_generator # added for testability
    attr_reader :intervals

    # intervals represent seconds from midnight
    def initialize(intervals, job, batch, window_seconds = 65)
      super(job, batch)
      @intervals = intervals
      @window_seconds = window_seconds
    end

    # Returns true if we should queue this job for executing given the rules
    # of this schedule object. Looking to run a job within the cron window.
    def ready?
      has_pending = ::ETL::Model::JobRunRepository.instance.has_pending?(@job, @batch)
      if has_pending
        log.debug("Job #{@job.id} is pending or running so not ready to run")
        return false
      end

      last_ended_job = ::ETL::Model::JobRunRepository.instance.last_ended(@job, @batch)
      last_ended = nil
      last_ended_mins_from_midnight = convert_seconds_from_midnight(last_ended_job.ended_at) unless last_ended_job.nil?
      now = get_current_time
      now_seconds_from_midnight = convert_seconds_from_midnight(now)

      # ensure that the last time the job was run was before now.
      @intervals.each do |initial_interval|
        start_interval = initial_interval - @window_seconds/2
        end_interval = start_interval + @window_seconds
        if !last_ended.nil? && last_ended.day == now.day && last_ended.month == now.month && last_ended.year == now.year && last_ended_mins_from_midnight > start_interval && last_ended_mins_from_midnight < end_window
          log.debug("Job #{@job.id} has already run within the current window")
          return false
        end

        if now_seconds_from_midnight > start_interval && now_seconds_from_midnight < end_interval
          log.debug("Job #{@job.id} should be enqueued to run as its window was found.")
          return true
        end
      end
      false
    end

    def convert_seconds_from_midnight(time)
      time.hour * 60 * 60 + time.min * 60 + time.sec
    end

    def get_current_time
      return @now_generator.now unless @now_generator.nil?
      Time.now
    end
  end

  class DailyTimesByInterval < DailyTimes
    def initialize(start_time, interval_mins, job, batch, window_seconds = 65)
      values = start_time.split(":")
      start_from_midnight_seconds = 0
      start_from_midnight_seconds = values[0].to_i * 60 * 60  if values.count > 0 # hours to minutes to seconds
      start_from_midnight_seconds += values[1].to_i * 60 if values.count > 1 # minutes to seconds
      start_from_midnight_seconds += values[2].to_i if values.count > 2 # minutes to seconds

      current = start_from_midnight_seconds
      intervals = []
      while  current < 86401
        intervals << current
        current += interval_mins * 60
      end
      super(intervals, job, batch, window_seconds)
    end
  end
end
