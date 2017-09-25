require_relative '../command'
require 'etl/job/exec'

module ETL::Cli::Cmd
  # Class that handles processing jobs from the queue as a background process
  class Worker < ETL::Cli::Command
    # Starts the infinite loop that processes jobs from the queue
    def execute
      notifier = ::ETL::Slack::Notifier.create_instance("etl_worker")

      begin
        ETL.load_user_classes
      rescue StandardError => e
        notifier.notify("loading jobs in etl worker failed: #{e.to_s}") unless notifier.nil?
        throw
      end
      ETL.queue.handle_incoming_messages
    end
  end
end
