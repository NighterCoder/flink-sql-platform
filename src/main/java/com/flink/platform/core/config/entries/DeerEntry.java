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

package com.flink.platform.core.config.entries;

import com.flink.platform.core.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.flink.platform.core.config.Environment.DEER_ENTRY;
import static com.flink.platform.core.config.Environment.SERVER_ENTRY;

/**
 * 用来加载 sql-platform-defaults.yml 文件
 */
public class DeerEntry extends ConfigEntry {

    public static final DeerEntry DEFAULT_INSTANCE = new DeerEntry(new DescriptorProperties(true));


    private static final String DEER_URL = "url";

    private static final String DEER_USERNAME = "username";

    private static final String DEER_PASSWORD = "password";


    private DeerEntry(DescriptorProperties properties) {
        super(properties);
    }

    @Override
    protected void validate(DescriptorProperties properties) {
        properties.validateString(DEER_URL, true);
        properties.validateString(DEER_USERNAME, true);
        properties.validateString(DEER_PASSWORD, true);
    }

    public static DeerEntry create(Map<String, Object> config) {
        return new DeerEntry(ConfigUtil.normalizeYaml(config));
    }

    public Map<String, String> asTopLevelMap() {
        return properties.asPrefixedMap(DEER_ENTRY + '.');
    }

    /**
     * Merges two session entries. The properties of the first execution entry might be
     * overwritten by the second one.
     */
    public static DeerEntry merge(DeerEntry gateway1, DeerEntry gateway2) {
        final Map<String, String> mergedProperties = new HashMap<>(gateway1.asTopLevelMap());
        mergedProperties.putAll(gateway2.asTopLevelMap());

        final DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(mergedProperties);

        return new DeerEntry(properties);
    }


    public String getDeerUrl() {
        return properties.getOptionalString(DEER_URL).orElse("");
    }

    public String getDeerUsername() {
        return properties.getOptionalString(DEER_USERNAME).orElse("");
    }

    public String getDeerPassword() {
        return properties.getOptionalString(DEER_PASSWORD).orElse("");
    }


}
