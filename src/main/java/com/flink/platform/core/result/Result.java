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

package com.flink.platform.core.result;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.sinks.TableSink;

import java.util.List;

/**
 * A result of a table program submission to a cluster.
 *
 * @param <C> type of the cluster id to which this result belongs to
 * @param <R> type of result data
 */
public interface Result<C, R> {

	/**
	 * Sets the cluster information of the cluster this result comes from. This method should only be called once.
	 */
	void setClusterInformation(C clusterId, String webInterfaceUrl);

	/**
	 * Starts the table program using the given deployer and monitors it's execution.
	 */
	void startRetrieval(JobClient jobClient);

	/**
	 * Retrieves the available result records.
	 */
	TypedResult<List<R>> retrieveChanges();

	/**
	 * Returns the table sink required by this result type.
	 */
	TableSink<?> getTableSink();

	/**
	 * Closes the retrieval and all involved threads.
	 */
	void close();

}
