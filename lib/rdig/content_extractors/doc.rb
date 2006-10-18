module RDig
  module ContentExtractors
      
    # Extract text from word documents
    #
    # Requires the wvHtml utility
    # (on debian and friends do 'apt-get install wv')
    class WordContentExtractor < ContentExtractor
      include ExternalAppHelper
      
      def initialize(config)
        super(config)
        @wvhtml = 'wvHtml'
        @pattern = /^application\/msword/
        # html extractor for parsing wvHtml output
        @html_extractor = RubyfulSoupContentExtractor.new(OpenStruct.new(
            :rubyful_soup => OpenStruct.new(
              :content_tag_selector => lambda { |tagsoup|
                tagsoup.html.body
              },
              :title_tag_selector         => lambda { |tagsoup|
                tagsoup.html.head.title
              }
            )))

        # TODO: better: if $?.exitstatus == 127 (not found)
        @available = %x{#{@wvhtml} -h 2>&1} =~ /Dom Lachowicz/
      end
      
      def process(content)
        result = {}
        as_file(content) do |file|  
          result = @html_extractor.process(%x{#{@wvhtml} --charset=UTF-8 '#{file.path}' -})
        end
        return result || {}
      end
      
    end

  end
end