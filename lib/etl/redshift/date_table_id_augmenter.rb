require 'etl/core'
require 'etl/transform/date_table_id_augmenter'

module ETL::Redshift

  class DateTableIDAugmenter < ::ETL::Transform::DateTableIDAugmenter
    def initialize(table_schema)
      date_columns = []
      table_schema.columns.each do |key, c|
        type = case c.type
        when :date
          date_columns << key
        when :datetz
          date_columns << key
        end
      end
      super(date_columns)
    end
  end
end
