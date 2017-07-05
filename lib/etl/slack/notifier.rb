require 'slack-notifier'

module ETL::Slack
  class Notifier 
    def initialize(webhook_url, channel, username)
      @notifier = Slack::Notifier.new(webhook_url, channel: channel, username: username)
    end

    def notify(message, icon_emoji: ":beetle:", attachments: [])
      @notifier.ping message, icon_emoji: icon_emoji, attachments: attachments
    end
  end
end