require 'fileutils'
require 'tempfile'

module ETL::Queue

  # Simple file-based queue system that's ok for use in testing environment
  class File << Base    
    def initialize(params)
      @fname = params.fetch("path", Tempfile.new("ETL::Queue::File").path)
    end
    
    def enqueue(payload)
      File.open(@fname, "a") do |f|
        fputs(payload.encode + "\n")
      end
    end

    # Removes all jobs from the queue
    def purge
      FileUtils.rm(@fname)
    end
    
    def status
      File.stat(@fname).to_s
    end

    # Process every line in our file on each iteration of the thread
    def process_async
      Thread.new do
        while true
          if File.exist?(@fname)
            tmp = Tempfile.new("ETL::Queue::File")
            # move to a temp file for processing in case any other lines
            # get added. This should prevent any race conditions as long
            # as the dirs are on the same filesystem
            FileUtils.mv(@fname, tmp.path)
            File.open(tmp.path, "r") do |f|
              while line = f.gets
                payload = ETL::Queue::Payload.decode(line)
                yield nil, payload
              end
            end
            tmp.unlink
          else
            # nothing in the queue - pause and then try again
            sleep(5) 
          end
        end
      end
    end
    
    def ack(msg_info)
      # do nothing
    end
  end
end
