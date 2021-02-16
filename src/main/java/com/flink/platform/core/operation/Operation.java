package com.flink.platform.core.operation;

import com.flink.platform.core.rest.result.ResultSet;

/**
 * Operation is a specific representation of a command (e.g. SELECT, SHOW, CREATE),
 * which could execute the command and return the result.
 */
public interface Operation {
    /**
     * Execute the command and return the result.
     */
    ResultSet execute();
}
