require 'etl/core'

module ETL::Redshift

  class DateTableIDAugmenter < ::ETL::Transform::DateTableIDAugmenter

    def initialize(client, table_name)
      table_schema = client.table_schema(table_name)
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
