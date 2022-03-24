/**
 WriteOperation.java
 Purpose: To create WriteOperation class
 @author Niharika Sinha (ns4451)
 */
package Operation;

import Transaction.Transaction;

public class WriteOperation extends Operation{
    public int value;

    public WriteOperation(String variable, Transaction t, int value, int time) {
        this.value = value;
        this.operation_time = time;
        this.variable = variable;
        this.transaction = t;
    }

}
