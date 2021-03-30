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

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.config.entries.TableEntry;
import com.flink.platform.core.config.entries.ViewEntry;
import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.exception.SqlExecutionException;
import com.flink.platform.core.rest.result.ResultSet;

/**
 * Operation for DROP VIEW command.
 */
public class DropViewOperation implements NonJobOperation {
	private final SessionContext context;
	private final String viewName;

	public DropViewOperation(SessionContext context, String viewName) {
		this.context = context;
		this.viewName = viewName;
	}

	@Override
	public ResultSet execute() {
		Environment env = context.getExecutionContext().getEnvironment();
		TableEntry tableEntry = env.getTables().get(viewName);
		if (tableEntry == null || !(tableEntry instanceof ViewEntry)) {
			throw new SqlExecutionException("'" + viewName + "' does not exist in the current session.");
		}

		// Here we rebuild the ExecutionContext because we want to ensure that all the remaining views can work fine.
		// Assume the case:
		//   view1=select 1;
		//   view2=select * from view1;
		// If we delete view1 successfully, then query view2 will throw exception because view1 does not exist. we want
		// all the remaining views are OK, so do the ExecutionContext rebuilding to avoid breaking the view dependency.
		Environment newEnv = env.clone();
		if (newEnv.getTables().remove(viewName) != null) {
			// Renew the ExecutionContext.
			ExecutionContext<?> newExecutionContext = context
				.createExecutionContextBuilder(context.getOriginalSessionEnv())
				.env(newEnv)
				.build();
			context.setExecutionContext(newExecutionContext);
		}

		return OperationUtil.AFFECTED_ROW_COUNT0;
	}
}
