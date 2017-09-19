module ETL::Redshift
  # Redshift batch factory will automatically find the start date
  # of that latest rows that need to get imported in based on the query.
  # TODO: in the future support batch payload imputs to generate a batch output
  class BatchFactory < ETL::BatchFactory::Base
    def initialize(get_start_time_query, backfill_days = 7, conn_params = nil, aws_params = nil)
      @get_start_time_query = get_start_time_query
      @backfill_days = backfill_days
      @conn_params = if conn_params.nil?
                       ::ETL.config.redshift[:etl]
                     else
                       conn_params
                     end
      @aws_params = if aws_params.nil?
                       ::ETL.config.aws[:etl]
                     else
                       aws_params
                     end
    end

    def generate
      client = Client.new(@conn_params, @aws_params)
      result = client.fetch(@get_start_time_query).map { |v| v }
      value = nil

      value = result[0][:date] if result.length == 1

      # set the end time to now
      end_time = Time.now.getutc
      b = ::ETL::Batch.new(end_time: end_time)
      value = Time.parse(value).getutc if value.is_a?(String)
      value = end_time - 60 * 60 * 24 * @backfill_days if value.nil?
      b.start_time = value
      b
    end
  end
end
