require 'etl/context'
module ETL
  
  def ETL.logger
    @@context.logger ||= ETL.create_logger
  end
  
  def ETL.create_logger
    log_cfg = ETL.app_config["log"]
    log_params = log_cfg.fetch("params", {})
    
    # figure out log location based on config
    log_path = log_params.fetch("log_dir", "log")
    unless log_path.start_with?("/")
      log_path = "#{ETL.root}/#{log_path}"
    end
    fname = "#{log_path}/#{ETL.context.env}.log"
    
    # instantiate logging class based on config
    class_name = log_cfg.fetch("class") do |k|
      raise "Missing log::#{k} in app configuration"
    end
    log = Object::const_get(class_name).new(fname)
    log.set_severity_string(log_params.fetch("level", "info"))

    log
  end
  
  def ETL.queue
    @@context.queue ||= ETL.create_queue
  end
  
  def ETL.create_queue
    queue_cfg = ETL.app_config["queue"]
    queue_params = queue_cfg.fetch("params", {})
    
    # instantiate queueing class based on config
    class_name = queue_cfg.fetch("class") do |k|
      raise "Missing queue::#{k} in app configuration"
    end
    Object::const_get(class_name).new(queue_params)
  end  
end  
