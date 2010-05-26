#!/usr/bin/env ruby

#--
# Copyright (c) 2006 Jens Kraemer
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#++
#

RDIGVERSION = '0.3.8'


require 'thread'
require 'thwait'
require 'singleton'
require 'monitor'
require 'ostruct'
require 'uri'
require 'cgi'
require 'set'
require 'net/http'
require 'getoptlong'
require 'tempfile'
require 'open-uri'
require 'logger'
require 'base64'

begin
  require 'ferret'
rescue LoadError
  require 'rubygems'
  require 'ferret'
  require 'em_models'
end


$:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

require 'rdig/content_extractors'
require 'rdig/url_filters'
require 'rdig/search'
require 'rdig/index'
require 'rdig/file'
require 'rdig/documents'
require 'rdig/crawler'
require 'em_models'

#require 'htmlentities/htmlentities'


$KCODE = 'u'
require 'jcode'

# See README for basic usage information
module RDig


  class << self
    def logger
      @logger ||= create_logger
    end

    def logger=(log)
      @logger = log
    end

    def create_logger
      l = Logger.new(STDOUT)
      l.level = Logger.const_get RDig.config.log_level.to_s.upcase rescue Logger::WARN
      return l
    end

    # Filter chains are used by the crawler to limit the set of documents being indexed.
    # There are two chains - one for http, and one for file system crawling.
    # Each document has to survive all filters in the relevant chain to get indexed.
    def filter_chain
      @filter_chain ||= {
              # filter chain for http crawling
              :http => [
                      :scheme_filter_http,
                      :fix_relative_uri,
                      {:normalize_uri => :normalize_uri},
                      {RDig::UrlFilters::DepthFilter => :max_depth},
                      {:hostname_filter => :include_hosts},
                      {RDig::UrlFilters::UrlInclusionFilter => :include_documents},
                      {RDig::UrlFilters::UrlExclusionFilter => :exclude_documents},
                      RDig::UrlFilters::VisitedUrlFilter
              ],
              # filter chain for file system crawling
              :file => [
                      :scheme_filter_file,
                      {RDig::UrlFilters::PathInclusionFilter => :include_documents},
                      {RDig::UrlFilters::PathExclusionFilter => :exclude_documents}
              ]
      }

    end

    def index_filter_chain
      @index_filter_chain ||= {
              # filter chain for http indexing
              :http => [
                      {RDig::UrlFilters::IndexUrlInclusionFilter => :index_include_documents},
                      {RDig::UrlFilters::IndexUrlExclusionFilter => :index_exclude_documents},
              ],
              # filter chain for file system indexing
              :file => [
                      {RDig::UrlFilters::PathInclusionFilter => :include_documents},
                      {RDig::UrlFilters::PathExclusionFilter => :exclude_documents}
              ]
      }

    end

  end
  class ShagBot
    def initialize(website)
      load_configfile(website.crawler_config_file)
    end

    def load_configfile(file)
      load File.expand_path(file)
      @config = ShagBot.configuration
    end

    def config()
      @config ||= RDig::ShagBot.configuration
    end

    def searcher
      @searcher ||= Search::Searcher.new(config.index)
    end

    def crawler
      @crawler ||= Crawler.new(config.crawler, config.index, logger)
    end

    # RDig configuration
    #
    # may be used with a block:
    #   RDig.configuration do |config| ...
    #
    # see doc/examples/config.rb for a commented example configuration
    def self.config

      @config ||= OpenStruct.new(
              :log_file  => '/tmp/rdig.log',
              :log_level => :warn,
              :crawler           => OpenStruct.new(
                      :start_urls        => ["http://localhost:3000/"],
                      :include_hosts     => ["localhost"],
                      :include_documents => nil,
                      :exclude_documents => nil,
                      :index_document    => nil,
                      :num_threads       => 2,
                      :max_redirects     => 5,
                      :max_depth         => nil,
                      :wait_before_leave => 10,
                      :http_proxy        => nil,
                      :http_proxy_user   => nil,
                      :http_proxy_pass   => nil,
                      :normalize_uri => OpenStruct.new(
                              :index_document => nil,
                              :remove_trailing_slash => nil
                      )
              ),
              :content_extraction  => OpenStruct.new(
                      # settings for html content extraction (hpricot)
              :hpricot      => OpenStruct.new(
                      # css selector for the element containing the page title
              :title_tag_selector => 'title',
              # might also be a proc returning either an element or a string:
              # :title_tag_selector => lambda { |hpricot_doc| ... }
              :content_tag_selector => 'body'
              # might also be a proc returning either an element or a string:
              # :content_tag_selector => lambda { |hpricot_doc| ... }
              )
              ),
              :index                 => OpenStruct.new(
                      :path                => "/tmp/index/",
                      :create              => true,
                      :handle_parse_errors => true,
                      :analyzer            => Ferret::Analysis::StandardAnalyzer.new,
                      :occur_default       => :must,
                      :default_field       => '*'
              )
      )
    end

    def self.configuration
      if block_given?
        yield config
      else
        self.config
      end
    end


    def logger
      @logger ||= create_logger
    end

    def logger=(log)
      @logger = log
    end

    def create_logger
      l = Logger.new(STDOUT)
      l.level = Logger.const_get RDig.config.log_level.to_s.upcase rescue Logger::WARN
      RDig.logger = l
      return l
    end

    def crawl()
      # begin
      self.crawler.run
      # rescue => x
      #   puts x.message
      # end
    end

    def get_urls(website)
      results = RDig.searcher.search("url:*")
      out = []
      results[:list].each { |result|
        out << result[:url]
      }
      return out
    end


    def create_pages(website)
      now = Time.now
      results = RDig.searcher.search("url:*")
      out = []
      results[:list].each { |result|
        p = Page.new
        p.url = result[:url]
        p.website = website
        p.last_shagged_at = now
        p.save
      }
      return out
    end


    def get_page_data(page, sym = :html)
      results = RDig.searcher.search("url:\"#{page.url}\"")
      if (results.size > 1)
        raise "More than one result for page %s"%[page.url]
      else
        raise "Symbol #{ sym } does not exist in key map for document" unless results[:list][0].has_key? sym
        return results[:list][0][sym]
      end
    end

    # returns http options for open_uri if configured
    def open_uri_http_options
      unless RDig::configuration.crawler.open_uri_http_options
        opts = {}
        if RDig::configuration.crawler.http_proxy
          opts[:proxy] = RDig::configuration.crawler.http_proxy
          if user = RDig::configuration.crawler.http_proxy_user
            pass = RDig::configuration.crawler.http_proxy_pass
            opts['Authorization'] = "Basic " + Base64.encode64("#{user}:#{pass}")
          end
        end
        RDig::configuration.crawler.open_uri_http_options = opts
      end
      return RDig::configuration.crawler.open_uri_http_options
    end
  end

=begin
class Application

  OPTIONS = [
          ['--config', '-c', GetoptLong::REQUIRED_ARGUMENT,
           "Read aplication configuration from CONFIG."],
          ['--help', '-h', GetoptLong::NO_ARGUMENT,
           "Display this help message."],
          ['--query', '-q', GetoptLong::REQUIRED_ARGUMENT,
           "Execute QUERY."],
          ['--version', '-v', GetoptLong::NO_ARGUMENT,
           "Display the program version."],
  ]

  # Application options from the command line
  def options
    @options ||= OpenStruct.new
  end

  # Display the program usage line.
  def usage
    puts "rdig -c configfile {options}"
  end

  # Display the rake command line help.
  def help
    usage
    puts
    puts "Options are ..."
    puts
    OPTIONS.sort.each do |long, short, mode, desc|
      if mode == GetoptLong::REQUIRED_ARGUMENT
        if desc =~ /\b([A-Z]{2,})\b/
          long = long + "=#{$1}"
        end
      end
      printf "  %-20s (%s)\n", long, short
      printf "      %s\n", desc
    end
  end

  # Return a list of the command line options supported by the
  # program.
  def command_line_options
    OPTIONS.collect { |lst| lst[0..-2] }
  end

  # Do the option defined by +opt+ and +value+.
  def do_option(opt, value)
    case opt
      when '--help'
        help
        exit
      when '--config'
        options.config_file = value
      when '--query'
        options.query = value
      when '--version'
        exit
      else
        fail "Unknown option: #{opt}"
    end
  end

  # Read and handle the command line options.
  def handle_options
    opts = GetoptLong.new(* command_line_options)
    opts.each { |opt, value| do_option(opt, value) }
  end

  def load_configfile
    load File.expand_path(options.config_file)
  end

  def load_website_configfile(website)
    load File.expand_path(website.crawler_config_file)
  end

  # Run the +rdig+ application.

  def crawl(website, refresh = false)
    begin

      if (refresh)
        load_website_configfile(website.crawler_config_file)
        @crawler = Crawler.new
        @crawler.run
        return true
      else
        return true
      end
    rescue => x
      puts x.message
    end
  end

  def get_urls(website)
    results = RDig.searcher.search("url:*")
    out = []
    results[:list].each { |result|
      out << result[:url]
    }
    return out
  end


  def get_page_data(page)
    results = RDig.searcher.search("url:\"#{page.url}\"")
    if (results.size > 1)
      raise "More than one result for page %s"%[page.url]
    else
      return results[:list][0][:data]
    end
  end

  def run
    puts "RDig version #{RDIGVERSION}"
    handle_options
    begin
      load_configfile
    rescue
      puts $!.backtrace
      fail "No Configfile found!\n#{$!}"

    end

    puts "using Ferret #{Ferret::VERSION}"

    if options.query
      # query the index
      puts "executing query >#{options.query}<"
      results = RDig.searcher.search(options.query)
      puts "total results: #{results[:hitcount]}"
      results[:list].each { |result|
        puts <<-EOF
#{result[:url]}
        #{result[:title]}
        #{result[:extract]}

        EOF
      }
    else
      # rebuild index
      @crawler = Crawler.new
      @crawler.run
    end
  end

end
=end
end

#require 'rdig/content_extractors'
#require 'rdig/url_filters'
#require 'rdig/search'
#require 'rdig/index'
#require 'rdig/file'
#require 'rdig/documents'
#require 'rdig/crawler'


#RDig.logger.sev_threshold = Logger::DEBUG
#puts RDig.logger.sev_threshold
#RDig.application.run
#w = Website.new
#w.crawler_config_file = "/Users/antoniogarcia-martinez/src/rdig/doc/examples/config.rb"
#w.domain = "http://www.acmeclimbing.com"
rdig = RDig::ShagBot.new(w)
#rdig.logger.sev_threshold = Logger::DEBUG

#rdig.crawl