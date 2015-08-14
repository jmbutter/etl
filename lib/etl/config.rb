require 'etl/context'
require 'psych'

module ETL

  # Directory of where config files are kept. Can be overriden per app.
  unless ETL.respond_to?("config_dir")
    def ETL.config_dir
      File.expand_path('../../../config', __FILE__)
    end
  end

  # Defines the location of db configuration file
  unless ETL.respond_to?("db_config_file")
    def ETL.db_config_file
      "#{ETL.config_dir}/database.yml"
    end
  end

  # returns array of DBs parsed from config file
  def ETL.db_config
    context.db_config ||= Psych.load_file(ETL.db_config_file)
  end

  # Defines the location of app configuration file
  unless ETL.respond_to?("app_config_file")
    def ETL.app_config_file
      "#{ETL.config_dir}/app.yml"
    end
  end

  # returns array of apps parsed from config file
  def ETL.app_config
    context.app_config ||= Psych.load_file(ETL.app_config_file)
  end


  # Defines the location of schema configuration file
  unless ETL.respond_to?("schema_config_file")
    def ETL.schema_config_file
      "#{ETL.config_dir}/schema.yml"
    end
  end

  # returns array of schemas parsed from config file
  def ETL.schema_config
    context.schema_config ||= Psych.load_file(ETL.schema_config_file)
  end

end
