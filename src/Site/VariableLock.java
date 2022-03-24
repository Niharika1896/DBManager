/**
 VariableLock.java
 Purpose:
 @author Niharika Sinha (ns4451)
 */

package Site;
import Lock.*;
import Transaction.*;

public class VariableLock {
    public Lock lock;
    public int time_acquired;

    public VariableLock(Lock lock, int time) {
        this.lock = lock;
        this.time_acquired = time;
    }

}