/**
 Lock.java
 Purpose: To create the lock class
 @author Niharika Sinha (ns4451)
 */
package Lock;

import Transaction.Transaction;

public class Lock {
    public LockType lock_type;
    public Transaction tr;

    public Lock() {
        this.lock_type = LockType.NO_LOCK;
        this.tr = null;
    }

    public Lock(LockType lt, Transaction t) {
        this.lock_type = lt;
        this.tr = t;
    }

}