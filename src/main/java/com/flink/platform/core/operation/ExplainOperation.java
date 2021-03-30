/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.platform.core.operation;

import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.exception.SqlExecutionException;
import com.flink.platform.core.rest.result.ColumnInfo;
import com.flink.platform.core.rest.result.ConstantNames;
import com.flink.platform.core.rest.result.ResultKind;
import com.flink.platform.core.rest.result.ResultSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

/**
 * Operation for EXPLAIN command.
 */
public class ExplainOperation implements NonJobOperation {
	private final ExecutionContext<?> context;
	private final String statement;

	public ExplainOperation(SessionContext context, String statement) {
		this.context = context.getExecutionContext();
		this.statement = statement;
	}

	@Override
	public ResultSet execute() {
		final TableEnvironment tableEnv = context.getTableEnvironment();
		// translate
		try {
			final Table table = createTable(context, tableEnv, statement);
			String explanation = context.wrapClassLoader(() -> tableEnv.explain(table));

			return ResultSet.builder()
					.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
					.columns(ColumnInfo.create(ConstantNames.SHOW_EXPLANATION_RESULT, new VarCharType(false, explanation.length())))
					.data(Row.of(explanation))
					.build();

		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	/**
	 * Creates a table using the given query in the given table environment.
	 */
	private <C> Table createTable(ExecutionContext<C> context, TableEnvironment tableEnv, String selectQuery) {
		// parse and validate query
		try {
			return context.wrapClassLoader(() -> tableEnv.sqlQuery(selectQuery));
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}
}
