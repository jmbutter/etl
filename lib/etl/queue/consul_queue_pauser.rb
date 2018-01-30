require 'etl/redshift/client'
require 'diplomat'

module ETL
  module Queue
    class ConsulQueueValueClient
      def initialize
        @consul_key_path = ENV["ETL_PAUSE_QUEUE_CONSUL_KEY_PATH"]
        @consul_key_path = 'etl/queue_paused' if @consul_key_path.nil?
      end

      def current_value
        value = ::Diplomat::Kv.get(@consul_key_path)
        !value.nil? && value.downcase == "true"
      end

      def pause
        ::Diplomat::Kv.put(@consul_key_path, "true")
      end

      def unpause
        ::Diplomat::Kv.put(@consul_key_path, "false")
      end
    end

    class ConsulQueuePauser
      def initialize
        @client = ::ETL::Queue::ConsulQueueValueClient.new
      end

      def pause_dequeueing?
        @client.current_value
      end

      def wait_seconds
        wait_seconds_str = ENV["ETL_PAUSE_STATE_WAIT_SECONDS"]
        return 30 if wait_seconds_str.nil?
        wait_seconds_str.to_i
      end
    end
  end
end
