/**
 Transaction.java
 Purpose: To create the transaction objects and its operations
 @author Niharika Sinha (ns4451)
 */
package Transaction;
import Operation.*;

import java.util.ArrayList;

public class Transaction {
    public int transaction_id;
    public ArrayList<Operation> transaction_operations;
    public int begin_time;
    public int end_time;
    public boolean readOnly;
    public boolean isEnded;

    public Transaction(int id, int time, boolean readOnly) {
        this.transaction_id = id;
        this.begin_time = time;
        this.readOnly = readOnly;
    }
}
