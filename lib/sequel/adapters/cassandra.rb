Sequel.require 'adapters/shared/cassandra'
require 'cql'

module Sequel
  # Top level module for holding all Cassandra modules and classes
  # for Sequel.
  module Cassandra
    class Database < Sequel::Database
      include Sequel::Cassandra::DatabaseMethods
      set_adapter_scheme :cassandra
      
      def connect(server)
        opts = server_opts(server)

        client = Cql::Client.connect(opts)
        client.use(opts[:default_keyspace] || :default)
        client
      end

      def disconnect_connection(c)
        c.close
      end

      # A keyspace must be created before an tables (column_families)
      # Can populate it.
      # DB.create_keyspace('test', {replication: {class: 'SimpleStrategy', replication_factor: 3}})
      # CREATE KEYSPACE test WITH REPLICATION = { 'class':'SimpleStrategy','replication_factor':'3'} 
      def create_keyspace(keyspace, opts = OPT)
        if_not_exist = opts[:if_not_exists]
        durable_writes = opts[:durable_writes]
        binding.pry
        raise Error, "Replication strategy must be defined!" unless opts.has_key?(:replication)
        replication = opts[:replication]

        raise Error, "Replication class must be defined!" unless replication.has_key?(:class)
        replication_strategy = replication[:class]
        replication_factor = replication[:replication_factor]

        sql = "CREATE KEYSPACE "
        sql << "IF NOT EXISTS " if if_not_exist
        sql << keyspace.to_s
        sql << " WITH REPLICATION = "
        sql << "{ "
        sql << replication.map {|k, v| "'#{k}':'#{v}'"}.join(",")
        sql << "} "
        sql << "AND DURABLE_WRITES = #{durable_writes}" if opts[:durable_writes]
        
        execute(sql)
      end

      def execute(sql, opts= {})
        consistency = {consistency: opts[:read_consistency] || server_opts(opts)[:default_read_consistency]}

        synchronize do |conn|
          r = log_yield(sql){conn.execute(sql, consistency)}
          yield(r) if block_given?
          r
        end
      end

      def execute_dui(sql, opts = {})
        consistency = {consistency: opts[:write_consistency] || server_opts(opts)[:default_write_consistency] || :any}

        synchronize do |conn|
          r = log_yield(sql){conn.execute(sql, consistency)}
          yield(r) if block_given?
          r
        end
      end

      # Cassandra's default adapter does not support transactions
      def begin_transaction(conn, opts=OPTS)
        super if @opts[:provider]
      end

      def commit_transaction(conn, opts=OPTS)
        super if @opts[:provider]
      end

      def rollback_transaction(conn, opts=OPTS)
        super if @opts[:provider]
      end

      # Overwrites the generic integer with :int
      def type_literal_generic_integer(column)
        :int
      end
      
      # Replaces the SELECT NULL AS "nil" from "table" LIMIT 1;
      # query with SELECT COUNT(*) from "table" query,
      # Since NULL isn't supported
      def _table_exists?(ds)
        ds.get{count(:*){}}
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self
      include Sequel::Cassandra::DatasetMethods

      def fetch_rows(sql)
        execute(sql) do |results|
          results.each_row do |result|
            @columns ||= result.keys

            yield result.symbolize_keys
          end
        end
        self
      end

      def symbolize_keys
        hash = self.dup
        hash.keys.each do |key|
          hash[(key.to_sym rescue key) || key] = delete(key)
        end
        hash
      end
    end
  end
end