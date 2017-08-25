require 'etl/core'
require 'etl/transform/date_table_id_augmenter'

module ETL::Redshift

  class DateTableIDAugmenter < ::ETL::Transform::DateTableIDAugmenter
    def initialize(table_schemas)
      date_columns = []
      table_schemas.each do |ts|
        ts.date_columns { |key| date_columns << key }
      end
      super(date_columns)
    end
  end
end
