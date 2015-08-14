require 'etl/context'
module ETL
  
  def ETL.logger
    @@context.logger ||= ETL.create_logger
  end
  
  def ETL.create_logger
    log_cfg = ETL.app_config["log"]
    
    # figure out log location based on config
    log_path = log_cfg.fetch("log_dir", "log")
    unless log_path.start_with?("/")
      log_path = "#{ETL.root}/#{log_path}"
    end
    fname = "#{log_path}/#{ETL.context.env}.log"
    
    # instantiate logging class based on config
    class_name = log_cfg.fetch("class") do |k|
      raise "Missing log::#{k} in app configuration"
    end
    log = Object::const_get(class_name).new(fname)
    log.set_severity_string(log_cfg.fetch("level", "info"))

    log
  end
end  
