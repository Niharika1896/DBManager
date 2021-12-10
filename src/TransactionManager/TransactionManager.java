/**
 TransactionManager.java
 Purpose: The TransactionManager facilitates all transaction operations between all sites
 @author Niharika Sinha (ns4451)
 */
package TransactionManager;
import Deadlock.Deadlock;
import Site.*;
import Transaction.*;
import Operation.*;
import IO.*;
import Lock.*;
import Deadlock.*;

import java.util.*;

public class TransactionManager {
    final int _NUMSITES = 10;
    Site [] sites = new Site[_NUMSITES];
    ArrayList<Transaction> transactions = new ArrayList<>();
    HashMap<String, ArrayList<Site>> variableSiteMapping = new HashMap<>();
    Queue<Operation> operationWaitQ = new LinkedList<>();
    int tick = 0;
    Deadlock deadLockObject = new Deadlock();

    /**
     * Initialises all the sites
     *
     * @param void
     * @return void
     * @sideEffects - site's variable-value map
     */
    public void site_init() {
        //System.out.println("Initializing sites");
        for(int i = 0; i < _NUMSITES; i++) {
            Site site = new Site(i+1);
            for(int var = 1; var<=20; var++) {
                String variable = "x"+var;
                if(var % 2 == 0) {
                    site.add_variable(variable, var*10, 0);
                    if(variableSiteMapping.containsKey(variable)) {
                        ArrayList<Site> list = variableSiteMapping.get(variable);
                        list.add(site);
                        variableSiteMapping.put(variable,list);
                    } else {
                        ArrayList<Site> list = new ArrayList<>();
                        list.add(site);
                        variableSiteMapping.put(variable,list);
                    }
                } else {
                    int desired_site_num = (var + 1) % 10;
                    if(desired_site_num == 0 )
                        desired_site_num = 10;
                    if(site.site_id == desired_site_num) {
                        site.add_variable(variable, var*10, 0);
                        if(variableSiteMapping.containsKey(variable)){
                            ArrayList<Site> list = variableSiteMapping.get(variable);
                            list.add(site);
                            variableSiteMapping.put(variable, list);
                        } else {
                            ArrayList<Site> list = new ArrayList<>();
                            list.add(site);
                            variableSiteMapping.put(variable, list);
                        }
                    }
                }
            }
            sites[i] = site;
        }
    }

    /**
     * Print values of site variables
     *
     * @param void
     * @return void
     * @sideEffects - site's variable-value map
     */
    public void printVariableSiteMapping() {
        for(Map.Entry<String, ArrayList<Site>> entry : variableSiteMapping.entrySet()) {
            System.out.print(entry.getKey()+" -> sites ");
            ArrayList<Site> var_site = variableSiteMapping.get(entry.getKey());
            for(int i = 0; i < var_site.size(); i++) {
                System.out.print(var_site.get(i).site_id+" ");
            }
        }
    }

    /**
     * Perform read operation on a variable by a transaction
     *
     * @param var, transaction object, time
     * @return void
     * @sideEffects - sites's variable-value map, operationWaitQ, deadLockObject
     */
    public void read(String var, Transaction t, int time) {

        Transaction dependentTransaction = null;
        boolean varHasWriteLockInQ = false;

        for (Operation op: operationWaitQ) {
            String variable = op.variable;
            if(variable != var) continue;
            String variable_className = op.getClass().getName();
            variable_className = variable_className.substring(variable_className.indexOf(".")+1);
            if(variable_className.equals("WriteOperation")) {
                varHasWriteLockInQ = true;
                dependentTransaction = op.transaction;
                break;
            }
        }
        if(!varHasWriteLockInQ) {
            boolean hasDepenedentTransaction = false;
            boolean allSitesDown = true;

            for(int i = 0 ; i < sites.length; i++) {
                boolean availableAtSite= false;
                boolean acquire = false;
                boolean varStaleAtSite;

                ArrayList<Site> list = variableSiteMapping.get(var);

                for(int j =0; j < list.size(); j++) {
                    if(list.get(j) == sites[i]) {
                        availableAtSite = true;
                        break;
                    }
                }

                if(!availableAtSite)
                    continue;

                if(!sites[i].site_status)
                    continue;
                else
                    allSitesDown = false;

                acquire = sites[i].canAcquireReadLock(var, t, time);
                //boolean siteUpInTimeRead = sites[i].siteUpInTime(var, t, time);
                //if(siteUpInTimeRead && variableSiteMapping.get(var).size() == 1)
                //System.out.println("Can acquire read lock "+acquire);

                boolean gettingNullFromDependentTr = false;
                if(!acquire) {
                    //System.out.println(sites[i].site_id);
                    dependentTransaction = sites[i].getDependentTransactionforRead(var, t, time);
                    //System.out.println("Dependent transaction "+dependentTransaction.transaction_id);
                    if(dependentTransaction == null) {
                        gettingNullFromDependentTr = true;
                    } else {
                        hasDepenedentTransaction = true;
                        break;
                    }

                    //System.out.println("dependent transaction "+dependentTransaction.transaction_id);

                }

                varStaleAtSite = sites[i].checkIfVariableIsStale(var);

                if(availableAtSite && gettingNullFromDependentTr) {
                    sites[i].acquireReadLock(var, t, time);
                    sites[i].addVisitedTransactionInSite(t);
                    break;
                }

                if(availableAtSite && acquire && !varStaleAtSite) {
                    sites[i].acquireReadLock(var, t, time);
                    sites[i].addVisitedTransactionInSite(t);
                    break;
                } else if(availableAtSite && acquire && varStaleAtSite) {
                    Operation op = new ReadOperation(var, t, time);
                    operationWaitQ.add(op);
                }
            }
            if(hasDepenedentTransaction) {
                Operation op = new ReadOperation(var, t, time);
                operationWaitQ.add(op);
                if(!deadLockObject.edgeExists(t.transaction_id, dependentTransaction.transaction_id))
                    System.out.println("T"+t.transaction_id+" is added to the Wait Queue because of Lock Conflict with T"+dependentTransaction.transaction_id);
                deadLockObject.addEdge(t.transaction_id, dependentTransaction.transaction_id);
                return;
            }
            if(allSitesDown) {
                Operation op = new ReadOperation(var, t, time);
                boolean alreadyPresentInOpQ = false;
                for(Operation oper : operationWaitQ) {
                    if(oper.variable == op.variable && oper.transaction.transaction_id == op.transaction.transaction_id && op.getClass().getName().equals((oper.getClass().getName()))) {
                        alreadyPresentInOpQ = true;
                        break;
                    }
                }
                if(!alreadyPresentInOpQ){
                    System.out.println("Transaction T"+t.transaction_id+" is waiting because all sites are down");
                }
                operationWaitQ.add(op);

            }
        } else {
            Operation op = new ReadOperation(var, t, time);
            operationWaitQ.add(op);
            if(!deadLockObject.edgeExists(t.transaction_id, dependentTransaction.transaction_id))
                System.out.println("T"+t.transaction_id+" is added to the Wait Queue because of Lock Conflict with T"+dependentTransaction.transaction_id);
            deadLockObject.addEdge(t.transaction_id, dependentTransaction.transaction_id);
        }
    }

    /**
     * Perform read only operation on a variable by a transaction
     *
     * @param var, transaction object, time
     * @return void
     * @sideEffects - sites's variable-value map, operationWaitQ
     */
    public void readOnly(String var, Transaction t, int time) {

        ArrayList<Site> variable_sites = variableSiteMapping.get(var);
        //System.out.println("Size "+variable_sites.size());
        if(variable_sites.size() == 1) {
            Site site = variable_sites.get(0);
            if(site.site_status && !site.checkIfVariableIsStale(var)) {
                site.getReadOnlyValue(var, t);
            } else {
                Operation op = new ReadOperation(var, t, time);
                boolean alreadyPresentInOpQ = false;
                for(Operation oper : operationWaitQ) {
                    if(oper.variable == op.variable && oper.transaction.transaction_id == op.transaction.transaction_id && op.getClass().getName().equals((oper.getClass().getName()))) {
                        alreadyPresentInOpQ = true;
                        break;
                    }
                }
                if(!alreadyPresentInOpQ)
                    System.out.println("Transaction T"+t.transaction_id+" is waiting because all sites are down");
                operationWaitQ.add(op);
            }
        } else {
            boolean readAtSomeSite = false;
            for(int i = 0; i < variable_sites.size(); i++) {
                if(variable_sites.get(i).site_status && !variable_sites.get(i).checkIfVariableIsStale(var)) {
                    readAtSomeSite = true;
                    int latest_time_updated_before_RO = sites[i].getReadOnlyTime(var, t);
                    boolean siteUpBetweenTime = sites[i].siteUpBetweenTime(latest_time_updated_before_RO, t.begin_time);
                    //System.out.println("Here2");
                    if(siteUpBetweenTime) {
                        //System.out.println("Here3");
                        variable_sites.get(i).getReadOnlyValue(var, t);
                        variable_sites.get(i).addVisitedTransactionInSite(t);
                        break;
                    }
                }

            }
            if(!readAtSomeSite) {
                Operation op = new ReadOperation(var, t, time);
                boolean alreadyPresentInOpQ = false;
                for(Operation oper : operationWaitQ) {
                    if(oper.variable == op.variable && oper.transaction.transaction_id == op.transaction.transaction_id && op.getClass().getName().equals((oper.getClass().getName()))) {
                        alreadyPresentInOpQ = true;
                        break;
                    }
                }
                if(!alreadyPresentInOpQ) {
                    System.out.println("Transaction T"+t.transaction_id+" is waiting because all sites are down");
                }
                operationWaitQ.add(op);
            }

        }
    }

    /**
     * Perform write operation on a variable by a transaction
     *
     * @param variable, transaction objec, time
     * @return void
     * @sideEffects - sites's variable-value map, operationWaitQ, deadLockObject
     */
    public void write(String variable, int value, Transaction t, int time) {
        ArrayList<Site> list = variableSiteMapping.get(variable);
        HashSet<Integer> waitForTransIDs = new HashSet<>();

        boolean getLockOnAllSites = true;
        boolean allSitesAreDown = true;

        for(int i = 0; i < list.size(); i++) {
            String acquire = "";
            if(list.get(i).site_status) {
                allSitesAreDown = false;
                //System.out.println("Calling canAcquireWriteLock at site "+list.get(i).site_id);
                acquire = list.get(i).canAcquireWriteLock(variable, t, time);
                //System.out.println("acquire "+acquire);
                if(acquire.equals("")) {
                    //System.out.println("can acquire lock at site "+list.get(i).site_id);
                    continue;
                } else {
                    getLockOnAllSites = false;
                    if(acquire.contains(",")){
                        String []trans = acquire.split(",");
                        for(String tran : trans) {
                            waitForTransIDs.add(Integer.parseInt(tran));
                        }
                    } else {
                        waitForTransIDs.add(Integer.parseInt(acquire));
                    }
                }
            }
        }

        if(allSitesAreDown) {
            Operation op = new WriteOperation(variable, t, value, time);
            boolean alreadyPresentInOpQ = false;
            for(Operation oper : operationWaitQ) {
                if(oper.variable == op.variable && oper.transaction.transaction_id == op.transaction.transaction_id && op.getClass().getName().equals((oper.getClass().getName()))) {
                    alreadyPresentInOpQ = true;
                    break;
                }
            }
            if(!alreadyPresentInOpQ) {
                System.out.println("Transaction T"+t.transaction_id+" is waiting because all sites are down");
            }
            operationWaitQ.add(op);
        } else {
//            for(int i = 0; i < list.size() ; i++) {
//                list.get(i).putDatainSiteCache(variable, value, t, time);
//            }
            if(getLockOnAllSites) {
                //System.out.println("T"+t.transaction_id+"Was able to get lock on all sites");
                //get write locks on all variables in all sites
                for(int i = 0; i < list.size() ; i++) {
                    if(list.get(i).site_status) {
                        list.get(i).acquireWriteLock(variable, t, time);
                        list.get(i).putDatainSiteCache(variable, value, t, time);
                        list.get(i).addVisitedTransactionInSite(t);
                        //System.out.println("T"+t.transaction_id+" acquiring lock on site "+list.get(i).site_id);
                    }
                }

            } else if(!getLockOnAllSites){

                Operation op = new WriteOperation(variable, t, value, time);
                operationWaitQ.add(op);

                //send waitForTransIDs to deadlock module
                for(int i = 0; i < waitForTransIDs.size(); i++) {
                    for(int id : waitForTransIDs) {
                        if(!deadLockObject.edgeExists(t.transaction_id, id))
                            System.out.println("T"+t.transaction_id+" is added to the Wait Queue because of Lock Conflict with T"+id);
                        deadLockObject.addEdge(t.transaction_id, id);
                    }
                }


            }
        }


    }

    /**
     * Fail a site at given time
     *
     * @param site_num, failtime
     * @return void
     * @sideEffects - site_status_history, site_status, site.clearLockTable
     */
    public void failSite(int site_num, int failtime) {

        //get the site
        Site site = null;
        for(int i = 0; i < sites.length; i++) {
            if(sites[i].site_id == site_num)
                site = sites[i];
        }

        //update the site status history
        int [][]uptime = site.site_status_history.getLast();
        site.site_status_history.removeLast();
        uptime[0][1] = failtime-1;
        site.site_status_history.addLast(uptime);

        //update the site status
        site.site_status = false;

        //clear the locktable of the site
        site.clearLockTable();

        //abort all transactions accessed
        site.abortAllAccessedTransactions();

    }

    /**
     * Recover a site at a given time
     *
     * @param site_num, recovertime
     * @return void
     * @sideEffects - site_status_history, site_status, site.variableStaleState
     */
    public void recoverSite(int site_num, int recovertime) {
        Site site = null;
        for(int i = 0; i < sites.length; i++) {
            if(sites[i].site_id == site_num)
                site = sites[i];
        }
        int [][]uptime = {{recovertime, Integer.MAX_VALUE}};
        site.site_status_history.addLast(uptime);

        site.site_status = true;

        site.makeAllVariablesStale();

        for(Map.Entry<String, ArrayList<Site>> entry : variableSiteMapping.entrySet()) {
            String var = entry.getKey();
            int size = entry.getValue().size();
            if(size == 1) {
                Site s = entry.getValue().get(0);
                if(s.site_id == site.site_id) {
                    s.setVariableStaleFalse(var);
                }
            }

        }

    }

    /**
     * Perform operations on transaction begin, check if transaction is read-only
     *
     * @param id, time, readOnly
     * @return void
     * @sideEffects - transactions
     */
    public void beginTransaction(int id, int time, boolean readOnly) {
        //int trans_id = transactions.size() + 1;
        //System.out.println("Transaction id added T"+id);
        if(readOnly) {
            System.out.print("Read Only transaction ");
        }
        System.out.println("T"+id+" begins");
        Transaction tr = new Transaction(id, time, readOnly);
        transactions.add(tr);
    }

    /**
     * Perform operations on transaction end
     *
     * @param id, time
     * @return void
     * @sideEffects - transactions
     */
    public void endTransaction(int id, int time) {
        Transaction trans = null;
        int trans_index = -1;
        for (int i = 0; i < transactions.size(); i++) {
            if (transactions.get(i).transaction_id == id) {
                trans = transactions.get(i);
                trans_index = i;
                break;
            }
        }
        if(trans_index == -1) {
            return;
        }
        boolean isAlreadyAborted = trans.isEnded;

        if (!isAlreadyAborted) {
            //System.out.println("Transactoin T"+id+" commits");
            trans.isEnded = true;
            trans.end_time = time;
            writeFromCacheToDB(trans);
            cleanupTransaction(trans, trans_index);
            IO io = new IO();
            io.print("T"+id+" commits as it did not abort, End T"+id);

        } else {
            cleanupTransaction(trans, trans_index);
            new IO().print("Transaction T"+trans.transaction_id+" aborted due to site failure");
        }
    }

    /**
     * Load the data from site cache to site variables
     *
     * @param transaction object
     * @return void
     * @sideEffects - site.site_variable_values
     */
    public void writeFromCacheToDB(Transaction t) {
        for(int i = 0; i < sites.length; i++) {
            Site site = sites[i];
            site.updateFromCacheToDB(t);
        }
    }

    /**
     * Cleanup transactions from wait queue, remove all locks of transaction
     *
     * @param transaction object, trans_index
     * @return void
     * @sideEffects - operationWaitQ, site.site_locktable
     */
    public void cleanupTransaction(Transaction t, int trans_index) {
        //System.out.println("cleanup of transaction T"+t.transaction_id);
        cleanWaitQForTransaction(t);
        removeAllTransactionLocks(t);
        transactions.remove(trans_index);
    }

    /**
     * Remove operations of a transaction from operationWaitQ
     *
     * @param transaction object
     * @return void
     * @sideEffects - operationWaitQ
     */
    public void cleanWaitQForTransaction(Transaction t) {
        ArrayList<Operation> list = new ArrayList<>();

        for(Operation op : operationWaitQ) {
            Transaction optrans = op.transaction;
            if(optrans.transaction_id == t.transaction_id) {
                list.add(op);
            }
        }
        operationWaitQ.removeAll(list);
    }

    /**
     * Remove all locks of a transaction
     *
     * @param transaction object
     * @return void
     * @sideEffects - site.site_locktable
     */
    public void removeAllTransactionLocks(Transaction tr) {
        for(int i = 0; i < sites.length; i++) {
            Site site = sites[i];
            site.removeAllTransactionLocks(tr);
        }
    }

    /**
     * Call dump function of the site to print its latest committed values of all variables
     *
     * @param void
     * @return void
     * @sideEffects - None
     */
    public void dump() {
        System.out.println("All sites dump:");
        for(Site site : sites){
            site.dumpVariables();
        }
    }

    /**
     * Process operationWaitQ at the beginning of every tick
     *
     * @param None
     * @return void
     * @sideEffects - operationWaitQ
     */
    public void processWaitQ() {
        while(!operationWaitQ.isEmpty()) {
            int size = operationWaitQ.size();
            Queue<Operation> q = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                Operation op = operationWaitQ.peek();
                operationWaitQ.poll();
                String operationType = op.getClass().getName();
                operationType = operationType.substring(operationType.indexOf(".")+1);
                if(operationType.equals("WriteOperation")) {
                    String var = op.variable;
                    Transaction t = op.transaction;
                    WriteOperation obj = (WriteOperation) op;
                    int val = obj.value;
                    boolean conflict = waitQueueWriteConflict(q, obj);
                    if(!conflict) {
                        write(var, val, t, tick);
                    }
                } else if(operationType.equals("ReadOperation")) {
                    String var = op.variable;
                    Transaction t = op.transaction;
                    if(t.readOnly) {
                        readOnly(var, t, tick);
                    } else {
                        ReadOperation obj = (ReadOperation) op;
                        boolean conflict = waitQueueReadConflict(q, obj);
                        if(!conflict) {
                            read(var, t, tick);
                        }
                    }
                }
                q.add(op);
            }
            int new_size = operationWaitQ.size();
            if(new_size == size)
                break;
        }
    }

    /**
     * Return transaction object given a transaction_id
     *
     * @param transaction_id
     * @return void
     * @sideEffects - None
     */
    public Transaction getTransactionFromTransactionID(int trans_id) {
        for(int i = 0 ; i < transactions.size(); i++) {
            if(transactions.get(i).transaction_id == trans_id) {
                return transactions.get(i);
            }
        }
        return null;
    }

    /**
     * Check if there exists a conflict between a new read operation and any existing operation of the queue
     *
     * @param queue, read operation op
     * @return boolean
     * @sideEffects - operationWaitQ
     */
    public boolean waitQueueReadConflict(Queue<Operation> queue, ReadOperation op) {
        for (Operation operationToCheck : queue) {
            if (operationToCheck instanceof ReadOperation) {
                continue;
            } else if (operationToCheck instanceof WriteOperation) {
                WriteOperation queueOperation = (WriteOperation) operationToCheck;
                if (!queueOperation.variable.equals(op.variable)) {
                    continue;
                }
                String var = op.variable;

                for (Site site : variableSiteMapping.get(var)) {
                    for (VariableLock varlock : site.site_locktable.get(var)) {
                        Lock lock = varlock.lock;
                        if (lock.tr == op.transaction) {
                            return false;
                        }
                    }
                }
                if(!deadLockObject.edgeExists(op.transaction.transaction_id, operationToCheck.transaction.transaction_id))
                    System.out.println("T"+op.transaction.transaction_id+" is added to the Wait Queue because of Lock Conflict with T"+operationToCheck.transaction.transaction_id);
                deadLockObject.addEdge(op.transaction.transaction_id, operationToCheck.transaction.transaction_id);
                operationWaitQ.add(op);
                return true;
            }
        }
        return false;
    }

    /**
     * Check if there exists a conflict between a new write operation and any existing operation of the queue
     *
     * @param queue, write operation op
     * @return boolean
     * @sideEffects - operationWaitQ
     */
    private boolean waitQueueWriteConflict(Queue<Operation> queue, WriteOperation op) {
        for (Operation operationToCheck : queue) {
            if (operationToCheck instanceof ReadOperation) {
                continue;
            } else if (operationToCheck instanceof WriteOperation) {
                WriteOperation queueOperation = (WriteOperation) operationToCheck;
                if (!queueOperation.variable.equals(op.variable)) {
                    continue;
                }
                String var = op.variable;
                for (Site site : variableSiteMapping.get(var)) {
                    for (VariableLock varlock : site.site_locktable.get(var)) {
                        Lock lock = varlock.lock;
                        if (lock.tr != op.transaction || lock.lock_type != LockType.WRITE_LOCK) {
                            if(!deadLockObject.edgeExists(op.transaction.transaction_id, operationToCheck.transaction.transaction_id))
                                System.out.println("T"+op.transaction.transaction_id+" is added to the Wait Queue because of Lock Conflict with T"+operationToCheck.transaction.transaction_id);
                            deadLockObject.addEdge(op.transaction.transaction_id, operationToCheck.transaction.transaction_id);
                            operationWaitQ.add(op);
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Detect deadlock and resolve deadlock if it exists by aborting victim transaction
     *
     * @param void
     * @return void
     * @sideEffects - transactions, deadlockObj
     */
    public void dealWithDeadlock() {
        //System.out.println("Checking for deadlock");
        boolean deadlockExists = deadLockObject.checkForDeadlock();
        if(deadlockExists) {
            System.out.println("Deadlock detected");
            Transaction victim = deadLockObject.resolveDeadlock(transactions);
            for(int i = 0 ; i < transactions.size(); i++) {
                if(victim == transactions.get(i)) {
                    victim.isEnded = true;
                    new IO().print("T"+victim.transaction_id+" aborted due to deadlock victim selection");
                    cleanupTransaction(victim, i);
                }
            }
        }
    }

    /**
     * System simulation
     *
     * @param IO object
     * @return void
     * @sideEffects - all Transaction Manager objects, all Sites objects
     */
    public void runSystem(IO io) {
        String line = "";
        while((line = io.readLine()) != null) {
            //System.out.println(line);

            dealWithDeadlock();
            processWaitQ();
            //System.out.println("Moving on");
            tick++;

            if(line.startsWith("beginRO")) {

                String transid = line.substring(line.indexOf("(")+2).trim();;
                transid = transid.substring(0, transid.length()-1);
                int transaction_id = Integer.parseInt(transid);
                beginTransaction(transaction_id, tick, true);

            } else if(line.startsWith("begin")) {

                String transid = line.substring(line.indexOf("(")+2).trim();;
                transid = transid.substring(0, transid.length()-1);
                int transaction_id = Integer.parseInt(transid);
                beginTransaction(transaction_id, tick, false);

            } else if(line.startsWith("end")) {

                String transid = line.substring(line.indexOf("(")+2).trim();;
                transid = transid.substring(0, transid.length()-1);
                int transaction_id = Integer.parseInt(transid);
                endTransaction(transaction_id, tick);

            } else if(line.startsWith("fail")) {

                String siteid = line.substring(line.indexOf("(")+1).trim();;
                siteid = siteid.substring(0, siteid.length()-1);
                int site_id = Integer.parseInt(siteid);
                failSite(site_id, tick);

            } else if(line.startsWith("recover")) {

                String siteid = line.substring(line.indexOf("(")+1);
                siteid = siteid.substring(0, siteid.length()-1).trim();;
                int site_id = Integer.parseInt(siteid);
                recoverSite(site_id, tick);

            } else if(line.startsWith("dump")) {

                dump();

            } else if(line.startsWith("W(")) {
                //W(T1,x1,100)
                String container = line.substring(line.indexOf("(")+1, line.indexOf(")"));
                //System.out.println(container+" - container");
                String []items = container.split(",");
                String transaction = items[0].trim();
                String variable = items[1].trim();
                int value = Integer.parseInt(items[2].trim());
                int transaction_id = Integer.parseInt(transaction.substring(1));
                Transaction t = getTransactionFromTransactionID(transaction_id);
                if(t == null) {

                } else  {
                    //System.out.println("Variable "+variable+" value "+value+" transaction id "+t.transaction_id);
                    WriteOperation writeop = new WriteOperation(variable, t, value, tick);
                    boolean conflict = waitQueueWriteConflict(operationWaitQ, writeop);
                    //System.out.println("Conflict :"+conflict);
                    if(!conflict) {
                        write(variable, value, t, tick);
                    }
                }

            } else if(line.startsWith("R(")) {

                String container = line.substring(line.indexOf("(")+1, line.indexOf(")"));
                String []items = container.split(",");
                String transaction = items[0].trim();;
                String variable = items[1].trim();;
                int transaction_id = Integer.parseInt(transaction.substring(1).trim());
                Transaction t = getTransactionFromTransactionID(transaction_id);
                if(t == null) {

                } else {
                    if(t.readOnly) {
                        readOnly(variable, t, tick);
                    } else {
                        ReadOperation readop = new ReadOperation(variable, t, tick);
                        boolean conflict = waitQueueReadConflict(operationWaitQ, readop);
                        if(!conflict) {
                            read(variable, t, tick);
                        }
                    }
                }
            }
        }

    }
}
