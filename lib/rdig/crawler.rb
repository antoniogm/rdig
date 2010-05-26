module RDig


  class Crawler

    def initialize(crawl_config = RDig::ShagBot.configuration.crawler, index_config = RDig::ShagBot.configuration.index, logger = RDig::ShagBot.logger)
      @documents = Queue.new
      @logger = logger
      @index_config = index_config
      @crawl_config = crawl_config
    end

    def run
      @indexer = Index::Indexer.new(@index_config)
      crawl
    ensure
      @indexer.close if @indexer
    end

    def crawl
      raise 'no start urls given!' if @crawl_config.start_urls.empty?
      # check whether we are indexing on-disk or via http
      url_type = @crawl_config.start_urls.first =~ /^file:\/\// ? :file : :http
      chain_config = RDig.filter_chain[url_type]
      index_chain_config = RDig.index_filter_chain[url_type]
      @etag_filter = ETagFilter.new
      filterchain = UrlFilters::FilterChain.new(chain_config, @crawl_config)
      indexfilterchain = UrlFilters::FilterChain.new(index_chain_config, @crawl_config)
      @crawl_config.start_urls.each { |url| add_url(url, filterchain) }

      num_threads = @crawl_config.num_threads
      group = ThreadsWait.new
      num_threads.times { |i|
        group.join_nowait Thread.new("fetcher #{i}") {
          filterchain = UrlFilters::FilterChain.new(chain_config, @crawl_config)
          indexfilterchain = UrlFilters::FilterChain.new(index_chain_config, @crawl_config)
          while (doc = @documents.pop) != :exit
            process_document doc, filterchain, indexfilterchain
          end
        }
      }

      # check for an empty queue every now and then 
      sleep_interval = @crawl_config.wait_before_leave
      begin
        sleep sleep_interval
      end until @documents.empty?
      # nothing to do any more, tell the threads to exit
      num_threads.times { @documents << :exit }

      @logger.info "waiting for threads to finish..."
      group.all_waits
    end

    def process_document(doc, filterchain, indexfilterchain = nil)
      @logger.debug "processing document #{doc}"
      doc.fetch
      case doc.status
        when :success
          if @etag_filter.apply(doc)
            # add links from this document to the queue
            doc.content[:links].each { |url|
              add_url(url, filterchain, doc)
            } unless doc.content[:links].nil?
            add_to_index doc, indexfilterchain
          end
        when :redirect
          @logger.debug "redirect to #{doc.content}"
          add_url(doc.content, filterchain, doc)
        else
          @logger.error "unknown doc status #{doc.status}: #{doc}"
      end
    rescue
      @logger.error "error processing document #{doc.uri.to_s}: #{$!}"
      @logger.debug "Trace: #{$!.backtrace.join("\n")}"
    end

    def add_to_index(doc, filterchain = nil)
      if (filterchain.nil?)
        @indexer << doc if doc.needs_indexing?
      else
        old_doc = doc
        doc = filterchain.apply(doc)

        if doc
          @indexer << doc if doc.needs_indexing?
          @logger.debug "document #{doc.uri} survived index filterchain"
        else
          @logger.debug "document #{old_doc.uri} excluded by index filterchain"

        end
      end
    end


    # pipes a new document pointing to url through the filter chain, 
    # if it survives that, it gets added to the documents queue for further
    # processing
    def add_url(url, filterchain, referring_document = nil)
      return if url.nil? || url.empty?

      @logger.debug "add_url #{url}"
      doc = if referring_document
        referring_document.create_child(url)
      else
        Document.create(url)
      end

      doc = filterchain.apply(doc)

      if doc
        @documents << doc
        @logger.debug "url #{url} survived filterchain"
      end
    rescue
      raise
    end

  end


  # checks fetched documents' E-Tag headers against the list of E-Tags
  # of the documents already indexed.
  # This is supposed to help against double-indexing documents which can 
  # be reached via different URLs (think http://host.com/ and 
  # http://host.com/index.html )
  # Documents without ETag are allowed to pass through
  class ETagFilter
    include MonitorMixin

    def initialize
      @etags = Set.new
      super
    end

    def apply(document)
      return document unless (document.respond_to?(:etag) && document.etag && !document.etag.empty?)
      synchronize do
        @etags.add?(document.etag) ? document : nil
      end
    end
  end

end
