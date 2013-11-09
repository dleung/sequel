module Sequel
  module Cassandra
    module DatabaseMethods
      def identifier_input_method_default
        nil
      end
    end

    module DatasetMethods
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'select distinct columns from where order limit')
      DELETE_CLAUSE_METHODS = Dataset.clause_methods(:delete, %w'delete from where')
      INSERT_CLAUSE_METHODS = Dataset.clause_methods(:insert, %w'insert into columns values')
      UPDATE_CLAUSE_METHODS = Dataset.clause_methods(:update, %w'update table set where')
      ARRAY_EMPTY = "[]".freeze
      CURLY_OPEN = "{".freeze
      CURLY_CLOSE = "}".freeze
      SET = Dataset::SET
      EQUAL = Dataset::EQUAL
      COMMA = Dataset::COMMA
      BRACKET_OPEN = Dataset::BRACKET_OPEN
      BRACKET_CLOSE = Dataset::BRACKET_CLOSE
      COLON = Dataset::COLON
      SPACE = Dataset::SPACE
      IS_OPERATORS = Dataset::IS_OPERATORS
      TWO_ARITY_OPERATORS = Dataset::TWO_ARITY_OPERATORS
      N_ARITY_OPERATORS = Dataset::N_ARITY_OPERATORS
      REGEXP_OPERATORS = Dataset::REGEXP_OPERATORS

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

      def update_set_sql(sql)
        values = opts[:values]
        sql << SET
        if values.is_a?(Hash)
          c = false
          eq = EQUAL
          values.each do |k, v|
            sql << COMMA if c
            if k.is_a?(String) && !k.is_a?(LiteralString)
              quote_identifier_append(sql, k)
            else
              literal_append(sql, k)
            end
            sql << eq

            if v.is_a?(Array)
              cass_array_sql_append(sql, v)
            elsif v.is_a?(Hash)
              cass_hash_sql_append(sql, v)
            else
              literal_append(sql, v)
            end
            c ||= true
          end
        else
          sql << values
        end
      end

      # For arrays datatypes, Cassandra uses brackets:
      # VALUES(['f@baggins.com','baggins@gmail.com','1'])
      def cass_array_sql_append(sql, a)
        # All values are treated as strings in an array
        a.each do |v|
          raise Error, "Array values must be a string: #{v}" unless v.is_a?(String)
        end
        if a.empty?
          sql << ARRAY_EMPTY
        else
          sql << BRACKET_OPEN
          expression_list_append(sql, a)
          sql << BRACKET_CLOSE
        end
      end

      def cass_hash_sql_append(sql, h)
        sql << CURLY_OPEN
        c = false
        co = COMMA
        h.each do |key, value|
          raise Error, "hash key must be a string" unless key.is_a?(String)
          raise Error, "hash value must be a string" unless value.is_a?(String)
          sql << co if c
          literal_append(sql, key)
          sql << COLON
          literal_append(sql, value)
          c ||= true
        end
        sql << CURLY_CLOSE
      end

      # Removes the LIMIT when doing a count.
      # In SQL, when performing a select count(*) the limit sets
      # the number of rows returned before.
      # By default, the limit applies to the row returned first
      # Before the count(*) is performed.
      # DB[:table].count # SELECT count(*) AS count FROM table
      def count(arg=(no_arg=true), &block)
        res = if no_arg
          if block
            arg = Sequel.virtual_row(&block)
            aggregate_dataset.select{count(arg){}.as(count)}
          else
            aggregate_dataset.select{count(:*){}.as(count)}
          end
        elsif block
          raise Error, 'cannot provide both argument and block to Dataset#count'
        else
            aggregate_dataset.select{count(arg){}.as(count)}
        end

        res.all.first[:count]
      end

      # Cassandra does not support regex
      def supports_regex?
        false
      end

      # Cassandra does not support IS TRUE
      def supports_is_true?
        false
      end
      
      private

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      def delete_clause_methods
        DELETE_CLAUSE_METHODS
      end

      def update_clause_methods
        UPDATE_CLAUSE_METHODS
      end

      def insert_clause_methods
        INSERT_CLAUSE_METHODS
      end
    end
  end
end