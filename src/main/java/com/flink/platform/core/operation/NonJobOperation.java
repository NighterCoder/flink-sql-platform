package com.flink.platform.core.operation;

/**
 * An operation just execute in local (will not submit a flink job), the corresponding command can be SHOW, CREATE...
 */
public interface NonJobOperation extends Operation {
}
