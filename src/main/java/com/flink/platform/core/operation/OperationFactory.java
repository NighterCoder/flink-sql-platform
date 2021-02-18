package com.flink.platform.core.operation;

import com.flink.platform.core.context.SessionContext;

/**
 * The factory to create {@link Operation} based on {@link SqlCommandParser.SqlCommandCall}.
 */
public class OperationFactory {

    public static Operation createOperation(SqlCommandParser.SqlCommandCall call, SessionContext context) {
        Operation operation;

        switch (call.command){
            case SELECT:
               // operation = new
        }

    }
}
