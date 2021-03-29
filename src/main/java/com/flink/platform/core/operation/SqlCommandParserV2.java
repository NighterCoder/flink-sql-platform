package com.flink.platform.core.operation;

import com.flink.platform.core.operation.version.FlinkShims;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * 改造SqlCommandParser,适应不同语义
 * <p>
 * Created by 凌战 on 2021/3/29
 */
public class SqlCommandParserV2 {


    private FlinkShims flinkShims;
    private Object tableEnv;

    public SqlCommandParserV2(FlinkShims flinkShims,Object tableEnv){
        this.flinkShims = flinkShims;
        this.tableEnv = tableEnv;
    }

    /**
     * SQL语句解析,利用传入的构造函数flinkShims来适配不同的语义
     *
     * @param stmt SQL语句
     */
    public Optional<SqlCommandCall> parse(String stmt) {
        return flinkShims.parseSql(tableEnv, stmt);
    }


    // --------------------------------------------------------------------------------------------

    private static final Function<String[], Optional<String[]>> NO_OPERANDS =
            (operands) -> Optional.of(new String[0]);

    private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
            (operands) -> Optional.of(new String[]{operands[0]});


    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;


    public enum SqlCommand {
        SELECT,

        INSERT_INTO,

        INSERT_OVERWRITE,

        CREATE_TABLE,

        ALTER_TABLE,

        DROP_TABLE,

        CREATE_VIEW,

        DROP_VIEW,

        CREATE_DATABASE,

        ALTER_DATABASE,

        DROP_DATABASE,

        USE_CATALOG,

        USE,

        SHOW_CATALOGS,

        SHOW_DATABASES,

        SHOW_TABLES,

        SHOW_FUNCTIONS,

        EXPLAIN,

        DESCRIBE_TABLE,

        RESET,

        // the following commands are not supported by SQL parser but are needed by users

		/*SET(
			"SET",
			// `SET` with operands can be parsed by SQL parser
			// we keep `SET` with no operands here to print all properties
			NO_OPERANDS),*/

        SET(
                "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
                (operands) -> {
                    if (operands.length < 3) {
                        return Optional.empty();
                    } else if (operands[0] == null) {
                        return Optional.of(new String[0]);
                    }
                    return Optional.of(new String[]{operands[1], operands[2]});
                }),

        // the following commands will be supported by SQL parser in the future
        // remove them once they're supported

        // FLINK-17396
        SHOW_MODULES(
                "SHOW\\s+MODULES",
                NO_OPERANDS),

        // FLINK-17111
        SHOW_VIEWS(
                "SHOW\\s+VIEWS",
                NO_OPERANDS),

        // the following commands are not supported by SQL parser but are needed by JDBC driver
        // these should not be exposed to the user and should be used internally

        SHOW_CURRENT_CATALOG(
                "SHOW\\s+CURRENT\\s+CATALOG",
                NO_OPERANDS),

        SHOW_CURRENT_DATABASE(
                "SHOW\\s+CURRENT\\s+DATABASE",
                NO_OPERANDS),


        CREATE_CATALOG(null, SINGLE_OPERAND),

        DROP_CATALOG(null, SINGLE_OPERAND),

        CREATE_FUNCTION(null, SINGLE_OPERAND),

        DROP_FUNCTION(null, SINGLE_OPERAND),

        ALTER_FUNCTION(null, SINGLE_OPERAND),

        DESCRIBE(
                "DESCRIBE\\s+(.*)",
                SINGLE_OPERAND),

        SOURCE(
                "SOURCE\\s+(.*)",
                SINGLE_OPERAND),

        HELP(
            "HELP",
            NO_OPERANDS),

		/*SHOW_CATALOGS(
				"SHOW\\s+CATALOGS",
				NO_OPERANDS),

		SHOW_CURRENT_CATALOG(
				"SHOW\\s+CURRENT\\s+CATALOG",
				NO_OPERANDS),

		SHOW_DATABASES(
				"SHOW\\s+DATABASES",
				NO_OPERANDS),

		SHOW_CURRENT_DATABASE(
				"SHOW\\s+CURRENT\\s+DATABASE",
				NO_OPERANDS),

		SHOW_TABLES(
				"SHOW\\s+TABLES",
				NO_OPERANDS),

		SHOW_FUNCTIONS(
				"SHOW\\s+FUNCTIONS",
				NO_OPERANDS),

		SHOW_MODULES(
				"SHOW\\s+MODULES",
				NO_OPERANDS),

		USE_CATALOG(
				"USE\\s+CATALOG\\s+(.*)",
				SINGLE_OPERAND),

		USE(
				"USE\\s+(?!CATALOG)(.*)",
				SINGLE_OPERAND),

		DESCRIBE(
				"DESCRIBE\\s+(.*)",
				SINGLE_OPERAND),

		EXPLAIN(
				"EXPLAIN\\s+(.*)",
				SINGLE_OPERAND),

		SELECT(
				"(WITH.*SELECT.*|SELECT.*)",
				SINGLE_OPERAND),

		INSERT_INTO(
				"(INSERT\\s+INTO.*)",
				SINGLE_OPERAND),

		INSERT_OVERWRITE(
				"(INSERT\\s+OVERWRITE.*)",
				SINGLE_OPERAND),

		CREATE_TABLE("(CREATE\\s+TABLE\\s+.*)", SINGLE_OPERAND),

		DROP_TABLE("(DROP\\s+TABLE\\s+.*)", SINGLE_OPERAND),

		IMPORT_LOOKUP("IMPORT\\s+LOOKUP\\s+(\\S+)", SINGLE_OPERAND),

		CREATE_VIEW(
				"CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)",
				(operands) -> {
					if (operands.length < 2) {
						return Optional.empty();
					}
					return Optional.of(new String[]{operands[0], operands[1]});
				}),

		CREATE_DATABASE(
				"(CREATE\\s+DATABASE\\s+.*)",
				SINGLE_OPERAND),

		DROP_DATABASE(
				"(DROP\\s+DATABASE\\s+.*)",
				SINGLE_OPERAND),

		DROP_VIEW(
				"DROP\\s+VIEW\\s+(.*)",
				SINGLE_OPERAND),

		ALTER_DATABASE(
				"(ALTER\\s+DATABASE\\s+.*)",
				SINGLE_OPERAND),

		ALTER_TABLE(
				"(ALTER\\s+TABLE\\s+.*)",
				SINGLE_OPERAND),

		SET(
				"SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
				(operands) -> {
					if (operands.length < 3) {
						return Optional.empty();
					} else if (operands[0] == null) {
						return Optional.of(new String[0]);
					}
					return Optional.of(new String[]{operands[1], operands[2]});
				}),

		RESET(
				"RESET",
				NO_OPERANDS);*/
        ;
        public final Pattern pattern;
        public final Function<String[], Optional<String[]>> operandConverter;

        SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
            this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
            this.operandConverter = operandConverter;
        }

        SqlCommand() {
            this.pattern = null;
            this.operandConverter = null;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        boolean hasPattern() {
            return pattern != null;
        }
    }

    /**
     * Call of SQL command with operands and command type.
     */
    public static class SqlCommandCall {
        public final SqlCommandParserV2.SqlCommand command;
        public final String[] operands;

        public SqlCommandCall(SqlCommandParserV2.SqlCommand command, String[] operands) {
            this.command = command;
            this.operands = operands;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlCommandParserV2.SqlCommandCall that = (SqlCommandParserV2.SqlCommandCall) o;
            return command == that.command && Arrays.equals(operands, that.operands);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(command);
            result = 31 * result + Arrays.hashCode(operands);
            return result;
        }

        @Override
        public String toString() {
            return command + "(" + Arrays.toString(operands) + ")";
        }
    }

}
