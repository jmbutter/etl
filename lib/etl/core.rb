# Pre-define the module so we can use simpler syntax
module ETL
end

require 'sequel'

# Core classes
require 'etl/logger.rb'
require 'etl/exception.rb'
require 'etl/jobs/result.rb'
require 'etl/jobs/base.rb'

# Models
# Set up the database connection that's needed for Sequel models
# Also we can use the DB constant in the rest of the code
DB = Sequel::Model.db = Sequel.connect(ETL.db_config[ETL.context.env])
Sequel::Model.plugin :timestamps
require 'etl/models/job_run_status.rb'
require 'etl/models/job.rb'
require 'etl/models/job_run.rb'

# Schema management
require 'etl/schema/table.rb'
require 'etl/schema/column.rb'

# Various ETL jobs
require 'etl/jobs/dummy.rb'
require 'etl/jobs/csv.rb'
require 'etl/jobs/sequel.rb'

# Input data readers
require 'etl/input/base.rb'
require 'etl/input/csv.rb'
require 'etl/input/array.rb'
require 'etl/input/sequel.rb'

# Row transforms
require 'etl/transform/base.rb'
require 'etl/transform/date_trunc.rb'
require 'etl/transform/map_to_nil.rb'
require 'etl/transform/zip5.rb'
