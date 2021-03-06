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
               operation = new SelectOperation(context,call.operands[0]);
               break;
            case SET:
                // list all properties
                if (call.operands.length == 0) {
                    operation = new SetOperation(context);
                } else {
                    // set a property
                    operation = new SetOperation(context, call.operands[0], call.operands[1]);
                }
                break;
            case SHOW_DATABASES:
                operation = new ShowDatabaseOperation(context);
                break;
            default:
                throw new RuntimeException("Unsupported command call " + call + ". This is a bug.");
        }



        return operation;
    }
}
