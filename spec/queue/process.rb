require 'etl/queue/process.rb'
RSpec.describe "process" do

  it "processes" do
    
    prc = ETL::Queue::Process.new(payload)
    jr = prc.run
  end
end
