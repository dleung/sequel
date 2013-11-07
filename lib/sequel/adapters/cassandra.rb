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
        client.use(opts[:default_keyspace])
        client
      end

      def disconnect_connection(c)
        c.close
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
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |results|
          results.each_row do |result|
            @columns ||= result.keys

            yield result
          end
        end
        self
      end

      # Overwrites the default behavior such that
      # The parenthesis are not added in the WHERE clauses.
      # "SELECT * FROM products WHERE cost == 10" is valid
      # "SELECT * FROM products where (cost == 10)" is invalid
      def complex_expression_sql_append(sql, op, args)
        case op
        when *IS_OPERATORS
          raise InvalidOperation, "IS expressions not supported"
        when *TWO_ARITY_OPERATORS
          if REGEXP_OPERATORS.include?(op) && !supports_regexp?
            raise InvalidOperation, "Pattern matching via regular expressions is not supported on #{db.database_type}"
          end
          literal_append(sql, args.at(0))
          sql << SPACE << op.to_s << SPACE
          literal_append(sql, args.at(1))
        when *N_ARITY_OPERATORS
          c = false
          op_str = " #{op} "
          args.each do |a|
            sql << op_str if c
            literal_append(sql, a)
            c ||= true
          end
        when :"NOT IN"
          raise InvalidOperation, "NOT IN expressions not supported"
        when :LIKE, :'NOT LIKE'
          raise InvalidOperation, "LIKE expressions not supported"
        when :ILIKE, :'NOT ILIKE'
          raise InvalidOperation, "ILIKE expressions not supported"
        else
          super
        end
      end

      # Cassandra does not support regex
      def supports_regex?
        false
      end

      # Cassandra does not support IS TRUE
      def supports_is_true?
        false
      end

      # Cassandra does not support multiple columns
      def supports_multiple_column_in?
        false
      end
    end
  end
end