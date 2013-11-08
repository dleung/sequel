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

      ARRAY_EMPTY = "[]".freeze
      CURLY_OPEN = "{".freeze
      CURLY_CLOSE = "}".freeze

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

      # For hashs (maps), it is in the format of
      # VALUES("{'a':'1','b':'2'}")
      def literal_hash_append(sql, v)
        sql << CURLY_OPEN
        hash_array_append(sql, v)
        sql << CURLY_CLOSE
      end

      def hash_array_append(sql, hash)
        c = false
        co = COMMA
        hash.each do |key, value|
          sql << co if c
          literal_append(sql, key)
          sql << COLON
          literal_append(sql, value)
          c ||= true
        end
      end

      # Cql requires the arrays to be in brackets, not parenthesis
      def literal_other_append(sql, v)
        case v
        when CassSqlArray
          array_cass_sql_append(sql, v)
        else
          super
        end
      end

      # For arrays datatypes, Cassandra uses brackets:
      # VALUES(['f@baggins.com','baggins@gmail.com','1'])
      def array_cass_sql_append(sql, a)
        # All values are treated as strings in an array
        a = a.map(&:to_s)
        if a.empty?
          sql << ARRAY_EMPTY
        else
          sql << BRACKET_OPEN
          expression_list_append(sql, a)
          sql << BRACKET_CLOSE
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