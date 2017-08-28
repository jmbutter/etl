require_relative './base'

module ETL::Transform

  class DateTableIDAugmenter < Base
    def initialize(date_columns)
      @date_columns = date_columns
      @date_truc = ::ETL::Transform::DateTrunc.new(:day)
    end

    def transform(row)
      @date_columns.each do |dc|
        date_value = row[dc]
        next if date_value.nil?

        if date_value.is_a? String
          date_value = DateTime.parse(date_value)
        end
        new_column_name = "#{dc}_dt_id"
        row[new_column_name] = date_value.strftime('%Y%m%d').to_i
      end
      row
    end
  end
end

