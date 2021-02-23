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

import com.flink.platform.core.rest.result.ColumnInfo;
import com.flink.platform.core.rest.result.ConstantNames;
import com.flink.platform.core.rest.result.ResultKind;
import com.flink.platform.core.rest.result.ResultSet;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for {@link Operation}.
 */
public class OperationUtil {

    public static final ResultSet AFFECTED_ROW_COUNT0 =
            ResultSet.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .columns(Collections.singletonList(ColumnInfo.create(ConstantNames.AFFECTED_ROW_COUNT, new BigIntType(false))))
                    .data(Collections.singletonList(Row.of(0L)))
                    .changeFlags(null)
                    .build();

    public static ResultSet singleStringToResultSet(String str, String columnName) {
        return ResultSet.builder()
                .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                .columns(Collections.singletonList((ColumnInfo.create(columnName, new VarCharType(false, str.length())))))
                .data(Collections.singletonList(Row.of(str)))
                .changeFlags(null)
                .build();
    }

    public static ResultSet stringListToResultSet(List<String> strings, String columnName) {
        List<Row> data = new ArrayList<>();
        int maxLength = VarCharType.DEFAULT_LENGTH;

        for (String str : strings) {
            maxLength = Math.max(str.length(), maxLength);
            data.add(Row.of(str));
        }

        List<ColumnInfo> columnInfos = Collections.singletonList(
                ColumnInfo.create(columnName, new VarCharType(false, maxLength)));
        return ResultSet.builder()
                .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                .columns(columnInfos)
                .data(data)
                .changeFlags(null)
                .build();
    }
}
