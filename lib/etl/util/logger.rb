require 'time'
require 'logger'

module ETL
  # Logger class that includes time stamp and severity for all messages
  class Logger < ::Logger
    attr_accessor :formatter

    def initialize(params = {})
      super(params[:file] || STDOUT)
      self.level = self.class.string_to_severity(params[:level])
      level_str = ENV["ETL_LOG_LEVEL"]
      env_level = self.class.string_to_severity(level_str) unless level_str.nil?
      self.level = env_level unless env_level.nil?
      @formatter = Formatter.new
    end
    
    alias_method :warning, :warn # convenience
    
    def context
      @formatter.context
    end
    
    def context=(h)
      @formatter.context = h
    end

    def exception(ex, severity = Logger::ERROR)
      add(severity) { self.class.create_exception_message(ex) }
    end

    def self.create_exception_message(ex)
      msg = "#{ex.class}: #{ex.message}:\n    "
      if ex.backtrace
        msg += ex.backtrace.join("    \n")
      else
        msg += "<no backtrace available>"
      end
      msg
    end

    # Converts string representation of severity into a Logger constant
    def self.string_to_severity(str)
      return ::Logger::INFO unless str
      case str.downcase
      when "debug"
        ::Logger::DEBUG
      when "info"
        ::Logger::INFO
      when "warning"
        ::Logger::WARN
      when "warn"
        ::Logger::WARN
      when "error"
        ::Logger::ERROR
      when "fatal"
        ::Logger::FATAL
      else
        ::Logger::INFO
      end
    end
    
    # Formatter that includes time stamp and severity. Also provides ability
    # to add job name and batch ID
    class Formatter < ::Logger::Formatter
      attr_accessor :context

      def initialize(ctx = {})
        @context = ctx
      end
      
      # convert context string into a prefix we can put in log messages
      def context_str
        a = @context.to_a.map{ |x| x.join(':') }.join(", ")
        a.empty? ? "" : "{#{a}} "
      end

      def call(severity, timestamp, progname, msg)
        str = String === msg ? msg : msg.inspect
        timestr = timestamp.strftime("%F %T.%L")
        "[#{timestr}] #{severity} #{context_str}#{str}\n"
      end
    end
  end
end
