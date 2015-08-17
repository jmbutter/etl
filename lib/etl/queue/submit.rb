




    # Returns the ActiveModel Job object
    def model()
      # return the model if we already have it cached in this instance
      return @model unless @model.nil?

      # get the model out of the DB
      @model = ETL::Model::Job.register(self.class.to_s())
    end
