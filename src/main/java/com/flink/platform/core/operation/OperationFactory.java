package com.flink.platform.core.operation;

import com.flink.platform.core.context.SessionContext;

/**
 * The factory to create {@link Operation} based on {@link SqlCommandParser.SqlCommandCall}.
 */
public class OperationFactory {

    public static Operation createOperation(SqlCommandParserV2.SqlCommandCall call, SessionContext context) {
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
            case CREATE_TABLE:
            case DROP_TABLE:
            case ALTER_TABLE:
            case CREATE_DATABASE:
            case DROP_DATABASE:
            case ALTER_DATABASE:
                operation = new DDLOperation(context, call.operands[0], call.command);
                break;
            case SHOW_TABLES:
                operation = new ShowTablesOperation(context);
                break;

            case INSERT_INTO:
            case INSERT_OVERWRITE:
                operation = new InsertOperation(context, call.operands[0], call.operands[1]);
                break;
            default:
                throw new RuntimeException("Unsupported command call " + call + ". This is a bug.");
        }



        return operation;
    }
}
