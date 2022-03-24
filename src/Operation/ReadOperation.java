/**
 ReadOperation.java
 Purpose: To create ReadOperation class
 @author Niharika Sinha (ns4451)
 */
package Operation;

import Transaction.Transaction;

public class ReadOperation extends Operation{

    public ReadOperation(String var, Transaction t, int time) {
        this.variable = var;
        this.transaction = t;
        this.operation_time = time;
    }

}
