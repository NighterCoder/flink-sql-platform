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
            case CREATE_VIEW:
                operation = new CreateViewOperation(context,call.operands[0],call.operands[1]);
                break;
            case DROP_VIEW:
                operation = new DropViewOperation(context,call.operands[0]);
                break;
            case USE_CATALOG:
                operation = new UseCatalogOperation(context,call.operands[0]);
                break;
            case USE:
                operation = new UseDatabaseOperation(context, call.operands[0]);
                break;
            case EXPLAIN:
                operation = new ExplainOperation(context,call.operands[0]);
                break;
            case SHOW_CATALOGS:
                operation = new ShowCatalogOperation(context);
                break;
            case SHOW_FUNCTIONS:
                operation = new ShowFunctionOperation(context);
                break;
            case RESET:
                operation = new ResetOperation(context);
                break;
            case SHOW_MODULES:
                operation = new ShowModuleOperation(context);
                break;
            case SHOW_CURRENT_CATALOG:
                operation = new ShowCurrentCatalogOperation(context);
                break;
            case SHOW_CURRENT_DATABASE:
                operation = new ShowCurrentDatabaseOperation(context);
                break;

            default:
                throw new RuntimeException("Unsupported command call " + call + ". This is a bug.");
        }



        return operation;
    }
}
