module ETL::Redshift
  class ValidatorBase
  	def initialize(client)
  	  @client = client
  	end

  	def validate(original_table, tmp_table, table_schema); end
  end
end
