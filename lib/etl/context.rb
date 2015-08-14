module ETL

  # class that stores app context for faster access e.g. so we don't have
  # to keep parsing YML files every time we reference config
  class Context
    attr_accessor :logger, :schema_config, :app_config, :db_config, :env
    def initialize
      @env = (ENV["ETL_ENV"] || 'development')
    end
  end
  
  # instantiate the context if it wasn't already
  @@context ||= Context.new
  
  # convenience method in this module to get the context
  def self.context
    @@context
  end
end
