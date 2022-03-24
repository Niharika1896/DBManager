/**
 Site.java
 Purpose: To create the manager which handles a particular site
 @author Niharika Sinha (ns4451)
 */
package Site;

import Lock.Lock;
import Lock.LockType;
import Transaction.*;
import IO.*;
import java.util.*;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;

public class Site {
    public int site_id;
    public boolean site_status;
    public HashMap<String, ArrayList<VariableLock>> site_locktable;
    public HashMap<String, ArrayList<VariableValue>> site_variable_values;
    public Deque<int[][]> site_status_history;
    public HashSet<Transaction> visited_transactions;
    HashMap<String, Boolean> variableStaleState;
    HashMap<Transaction, HashMap<String, List<List<Integer>>>> site_cache = new HashMap<>();

    public Site(int id) {
        this.site_id = id;
        this.site_status = true;
        site_status_history = new LinkedList<>();
        site_variable_values = new HashMap<>();
        site_locktable = new HashMap<>();
        site_status_history.addLast(new int[][]{{0, Integer.MAX_VALUE}});
        visited_transactions = new HashSet<>();
        variableStaleState = new HashMap<>();
    }

    /**
     * Loads the data temporarily in site cache
     *
     * @param variable, value, transaction_obj, time
     * @return void
     * @sideEffects - site_variable_values
     */
    public void putDatainSiteCache(String variable, int value, Transaction t, int time) {
        if(site_cache.containsKey(t)) {

            HashMap<String, List<List<Integer>>> temp = site_cache.get(t);
            if(temp.containsKey(variable)) {
                List<List<Integer>> l = temp.get(variable);
                List<Integer> li = new ArrayList<>();
                li.add(value);
                li.add(time);
                li.add(time);
                l.add(li);
                temp.put(variable, l);
            } else {
                List<List<Integer>> l = new ArrayList<>();
                List<Integer> li = new ArrayList<>();
                li.add(value);
                li.add(time);
                l.add(li);
                temp.put(variable, l);
            }
            site_cache.put(t, temp);
        } else {
            //System.out.println("Adding to Site "+site_id+" variable "+variable+" in its cache");
            //System.out.println("Transaction id : "+t.transaction_id);
            HashMap<String, List<List<Integer>>> temp = new HashMap<>();
            List<List<Integer>> l = new ArrayList<>();
            List<Integer> li = new ArrayList<>();
            li.add(value);
            li.add(time);
            l.add(li);
            temp.put(variable, l);
            site_cache.put(t, temp);
        }
    }

    /**
     * Update the data of a particular site from its cache
     *
     * @param transaction_obj
     * @return void
     * @sideEffects - site_variable_values
     */
    public void updateFromCacheToDB(Transaction t) {
        if(site_cache.containsKey(t)) {
            HashMap<String, List<List<Integer>>> temp = site_cache.get(t);
            for(Map.Entry<String, List<List<Integer>>> entry: temp.entrySet()) {
                String variable = entry.getKey();
                //System.out.println("Variable for T"+t.transaction_id+" is "+variable);
                ArrayList<VariableValue> varval = site_variable_values.get(variable);
                List<List<Integer>> list_variable_values = entry.getValue();
                for(int i = 0 ; i < list_variable_values.size(); i++) {
                    List<Integer> val_time = list_variable_values.get(i);
                    System.out.println("Committing variable "+variable+" on site "+this.site_id+" with value "+val_time.get(0));
                    VariableValue varval_obj = new VariableValue(val_time.get(0), val_time.get(1));
                    varval.add(varval_obj);
                }
                site_variable_values.put(variable, varval);
                variableStaleState.put(variable, false);
            }
        }
    }

    /**
     * Add a variable to a site
     *
     * @param var, value, time
     * @return void
     * @sideEffects - site_variable_values
     */
    public void add_variable(String var, int value, int time) {
        if(!site_locktable.containsKey(var)) {
            ArrayList<VariableLock> locklist = new ArrayList<>();
            locklist.add(new VariableLock(new Lock(LockType.NO_LOCK, null), time));
            site_locktable.put(var, locklist);
        } else {
            ArrayList<VariableLock> locklist = site_locktable.get(var);
            locklist.add(new VariableLock(new Lock(LockType.NO_LOCK, null), time));
            site_locktable.put(var, locklist);
        }

        if(!site_variable_values.containsKey(var)) {
            ArrayList<VariableValue> vallist = new ArrayList<>();
            vallist.add(new VariableValue(value, time));
            site_variable_values.put(var, vallist);
        } else {
            ArrayList<VariableValue> vallist = site_variable_values.get(var);
            vallist.add(new VariableValue(value, time));
            site_variable_values.put(var, vallist);
        }

        variableStaleState.put(var, false);
    }

    /**
     * Check if site was up in between given time
     *
     * @param variable, transaction object, time
     * @return boolean
     * @sideEffects - None
     */
    public boolean siteUpInTime(String variable, Transaction t, int time) {
        ArrayList<VariableValue> varlist = site_variable_values.get(variable);
        int varLatestCommitTime = -1;
        for(int i = 0; i < varlist.size(); i++) {
            VariableValue varval = varlist.get(i);
            if(varval.time_updated <= time) {
                varLatestCommitTime = varval.time_updated;
            }
        }
        boolean siteUpInTime = siteUpBetweenTime(varLatestCommitTime, time);
        return siteUpInTime;
    }

    /**
     * Checks if a read lock on a variable by a transaction can be acquired
     *
     * @param variable, transaction object,time
     * @return boolean
     * @sideEffects - None
     */
    public boolean canAcquireReadLock(String variable, Transaction t, int time) {
//        System.out.println(variable+" is variable");
//        System.out.println("Site is  "+site_id);
        ArrayList<VariableLock> locklist = site_locktable.get(variable);
        boolean varHasWriteLock = false;
        for(int i = 0; i < locklist.size(); i++) {
            VariableLock varLock = locklist.get(i);
            Lock lk = varLock.lock;
            if(lk.lock_type == LockType.WRITE_LOCK && lk.tr.transaction_id != t.transaction_id) {
                varHasWriteLock = true;
                break;
            }
        }
        ArrayList<VariableValue> varlist = site_variable_values.get(variable);
        int varLatestCommitTime = -1;
        for(int i = 0; i < varlist.size(); i++) {
            VariableValue varval = varlist.get(i);
            if(varval.time_updated <= time) {
                varLatestCommitTime = varval.time_updated;
            }
        }
        //boolean siteUpInTime = siteUpBetweenTime(varLatestCommitTime, time);
        //System.out.println("siteUpInTime "+siteUpInTime);
        //System.out.println("varHasWriteLock "+varHasWriteLock);
        if(!varHasWriteLock)
            return true;
        return false;

    }

    /**
     * Checks if a write lock on a variable by transaction can be acquired
     *
     * @param variable, transaction object,time
     * @return String
     * @sideEffects - None
     */
    public String canAcquireWriteLock(String variable, Transaction t, int time) {
        String waitForTransList = "";
        //System.out.println("Variable "+variable);
        ArrayList<VariableLock> locklist = site_locktable.get(variable);
        for(int i = 0; i < locklist.size(); i++) {
            VariableLock varLock = locklist.get(i);
            Lock lk = varLock.lock;
            if(locklist.get(i).lock.lock_type == LockType.NO_LOCK)
                continue;
            if((lk.lock_type == LockType.WRITE_LOCK) || (lk.lock_type == LockType.READ_LOCK && lk.tr.transaction_id != t.transaction_id)) {
                if(waitForTransList.equals("")){
                    waitForTransList = String.valueOf(lk.tr.transaction_id);
                } else {
                    waitForTransList = waitForTransList + "," + String.valueOf(lk.tr.transaction_id);
                }
            }
        }
        return waitForTransList;
    }

    /**
     * Acquires a write lock on a variable by a transaction
     *
     * @param variable, transaction object,time
     * @return void
     * @sideEffects - site_locktable
     */
    public void acquireWriteLock(String variable, Transaction t, int time) {
        ArrayList<VariableLock> list = site_locktable.get(variable);
        //System.out.println("Variable lock list size "+list.size()+" at site "+this.site_id);

        int index_to_change = -1;

        for(int i = 0; i < list.size(); i++) {
            if(list.get(i).lock.lock_type == LockType.READ_LOCK && list.get(i).lock.tr.transaction_id == t.transaction_id) {
                index_to_change = i;
            }
        }
        if(index_to_change == -1) {
            Lock lk = new Lock(LockType.WRITE_LOCK, t);
            VariableLock varlock = new VariableLock(lk, time);
            list.add(varlock);
            site_locktable.put(variable, list);
        } else {
            //upgrading read to write lock
            list.get(index_to_change).time_acquired = time;
            list.get(index_to_change).lock.lock_type = LockType.WRITE_LOCK;
        }
        //System.out.println("Variable lock list size "+list.size()+" at site "+this.site_id);
    }

    /**
     * Acquires a read lock on a variable by a transaction
     *
     * @param variable, transaction object, time
     * @return void
     * @sideEffects - site_locktable
     */
    public void acquireReadLock(String variable, Transaction t, int time) {
        int value = -99;
        ArrayList<VariableLock> list = site_locktable.get(variable);
        Lock lk = new Lock(LockType.READ_LOCK, t);
        VariableLock varlock = new VariableLock(lk, time);
        list.add(varlock);
        site_locktable.put(variable, list);

        ArrayList<VariableValue> vallist = site_variable_values.get(variable);

        int i;
        for(i = 0; i < vallist.size(); i++) {
            if(time > vallist.get(i).time_updated) {
                value = vallist.get(i).value;
            }
            else
                break;
        }

        printValueRead(t, variable, value);
    }

    /**
     * Check if a variable at site is stale
     *
     * @param variable
     * @return boolean
     * @sideEffects - None
     */
    public boolean checkIfVariableIsStale(String var) {
        return variableStaleState.get(var);
    }

    /**
     * Mark all variables of a site as stale
     *
     * @param - None
     * @return void
     * @sideEffects - variableStaleState
     */
    public void makeAllVariablesStale() {
        //get all variables occuring only in this site
        for(String var : variableStaleState.keySet()) {
            variableStaleState.put(var, true);
        }
    }

    /**
     * Mark a particular variable as not stale
     *
     * @param variable
     * @return void
     * @sideEffects - variableStaleState
     */
    public void setVariableStaleFalse(String var) {
        variableStaleState.put(var, false);
    }

    /**
     * Clear the locktable of site on failure
     *
     * @param - None
     * @return void
     * @sideEffects - site_locktable
     */
    public void clearLockTable() {
        for(String key : site_locktable.keySet()){
            site_locktable.put(key, new ArrayList<>());
        }
    }

    /**
     * Print value of a variable
     *
     * @param transaction object, variable, value
     * @return void
     * @sideEffects - None
     */
    public void printValueRead(Transaction t, String variable, int value) {
        //System.out.println("Here");
        IO io = new IO();
        io.printReadValue(t, variable, value);
    }

    /**
     * Get the correct value for Read Only transaction
     *
     * @param variable, transaction object
     * @return void
     * @sideEffects - None
     */
    public void getReadOnlyValue(String variable, Transaction t) {
        ArrayList<VariableValue> list = site_variable_values.get(variable);
        int value = -1;
        for(int i =0 ;i < list.size(); i++) {
            VariableValue varval = list.get(i);
            if(varval.time_updated < t.begin_time) {
                value = varval.value;
            } else {
                break;
            }
        }
        printValueRead(t, variable, value);
    }

    /**
     * Get the correct time for reading a variable for Read Only transaction
     *
     * @param variable, transaction object
     * @return int
     * @sideEffects - None
     */
    public int getReadOnlyTime(String variable, Transaction t) {
        ArrayList<VariableValue> list = site_variable_values.get(variable);
        int time = -1;
        for(int i =0 ;i < list.size(); i++) {
            VariableValue varval = list.get(i);
            if(varval.time_updated < t.begin_time) {
                time = varval.time_updated;
            } else {
                break;
            }
        }
        return time;
    }

    /**
     * Check if site is up between given time
     *
     * @param time1, time2
     * @return boolean
     * @sideEffects - None
     */
    public boolean siteUpBetweenTime(int time1, int time2) {
        //Deque<int[][]> site_status_history
        boolean siteUpInTime = false;
        for(int[][] it : site_status_history){
            int otime1 = it[0][0];
            int otime2 = it[0][1];
            if(time1 >= otime1 && time1 <= otime2) {
                if(time2 >= otime1 && time2 <= otime2){
                    siteUpInTime = true;
                    break;
                }
            }

        }
        return siteUpInTime;
    }

    /**
     * Release all locks of a particular transaction on a site
     *
     * @param transaction
     * @return void
     * @sideEffects - site_locktable
     */
    public void removeAllTransactionLocks(Transaction trans) {
        for(Map.Entry<String, ArrayList<VariableLock>> entry : site_locktable.entrySet()){
            String variable = entry.getKey();
            ArrayList<VariableLock> list = entry.getValue();
            ArrayList<VariableLock> indexList = new ArrayList<>();
            for(int i = 0; i < list.size(); i++) {
                VariableLock varLock = list.get(i);
                if(varLock.lock.lock_type == LockType.NO_LOCK)
                    continue;
                if(varLock.lock.tr.transaction_id == trans.transaction_id) {
                    indexList.add(varLock);
                }
            }
            list.removeAll(indexList);
            site_locktable.put(variable, list);
        }
    }

    /**
     * Add transaction that accessed a site to visited list
     *
     * @param transaction object
     * @return void
     * @sideEffects - visited_transactions
     */
    public void addVisitedTransactionInSite(Transaction t) {
        visited_transactions.add(t);
    }

    /**
     * Write a value to a variable
     *
     * @param transaction object, variable, value, time
     * @return void
     * @sideEffects - None
     */
    public void writeValueToVariable(Transaction trans, String variable, int value, int time) {

        ArrayList<VariableValue> varval = site_variable_values.get(variable);
        VariableValue varval_obj = new VariableValue(value, time);
        varval.add(varval_obj);
        site_variable_values.put(variable, varval);
        variableStaleState.put(variable, false);

    }

    /**
     * Abort all transactions that accessed the site
     *
     * @param void
     * @return void
     * @sideEffects - visited_transactions
     */
    public void abortAllAccessedTransactions() {
        for(Transaction t: visited_transactions) {
            //System.out.println("Accessed transaction T"+t.transaction_id);
            t.isEnded = true;
        }

    }

    /**
     * Print the latest committed value of all variables of a site
     *
     * @param void
     * @return void
     * @sideEffects - None
     */
    public void dumpVariables() {
        System.out.print("site " + this.site_id + " - ");

        ArrayList<String> keys = new ArrayList<>();
        for(String key : this.site_variable_values.keySet()) {
            keys.add(key);
        }
        Collections.sort(keys, new Comparator<String>() {
            public int compare(String s1, String s2) {
                return parseInteger(s1) - parseInteger(s2);
            }

            int parseInteger(String s) {
                String num = s.replaceAll("\\D", "");
                return num.isEmpty() ? 0 : Integer.parseInt(num);
            }
        });

        for(String s : keys) {
            ArrayList<VariableValue> varval = site_variable_values.get(s);
            int value = varval.get(varval.size()-1).value;
            System.out.print(s + ": " + value + " ");
        }

        System.out.println();
    }

    /**
     * Get all transactions to be added in wait-for graph for a read transaction
     *
     * @param variable, transaction object, time
     * @return Transaction
     * @sideEffects - None
     */
    public Transaction getDependentTransactionforRead(String variable, Transaction t, int time) {
        ArrayList<VariableLock> varlock = site_locktable.get(variable);
        for(int i = 0 ; i < varlock.size(); i++) {
            VariableLock vlock = varlock.get(i);
            Transaction temp = vlock.lock.tr;
            LockType ltype = vlock.lock.lock_type;
            if(ltype == LockType.WRITE_LOCK && !temp.isEnded) {
                return temp;
            }
        }
        return null;
    }

}
