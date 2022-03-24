/**
 Deadlock.java
 Purpose: To create wait for graph and check deadlock
 @author Niharika Sinha (ns4451)
 */

package Deadlock;
import Transaction.*;

import java.util.*;


public class Deadlock {
    public ArrayList<Integer> getAffectedNodes() {
        return affectedNodes;
    }

    private ArrayList<Integer> affectedNodes;
    private static final int totalVertices = 420;
    private List<List<Integer>> adj;

    public boolean edgeExists(int source, int destination){
        List<Integer> list = adj.get(source);
        for(int i = 0 ; i < list.size(); i++) {
            if(list.get(i) == destination)
                return true;
        }
        return false;

    }

    public Deadlock(){
        affectedNodes = new ArrayList<>();
        adj = new ArrayList<>();
        for (int i = 0; i < totalVertices; i++) {
            adj.add(new LinkedList<>());
        }
    }

    /**
     * Check for deadlock in the graph
     *
     * @param void
     * @return boolean
     * @sideEffects - None
     */
    public boolean checkForDeadlock() {
        boolean[] visited = new boolean[totalVertices];
        boolean[] recursionStack = new boolean[totalVertices];

        for (int i = 0; i < totalVertices; i++) {
            affectedNodes.clear();
            if (dfs(i, visited, recursionStack)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Add edge to the graph
     *
     * @param source vertex
     * @param destination vertex
     * @return void
     * @sideEffects - adjacency list of the graph
     */
    public void addEdge(int source, int destination) {

        adj.get(source).add(destination);
    }

    /**
     * Remove all edges from and to the source
     *
     * @param source vertex
     * @return void
     * @sideEffects - adjacency list of the graph
     */
    public void removeEdge(int source) {
        adj.get(source).clear();
        for (int i = 0; i < adj.size(); i++) {
            adj.get(i).removeAll(Collections.singleton(source));
        }
    }

    /**
     * Resolve deadlock in the graph
     *
     * @param transactions
     * @return earliest transaction
     * @sideEffects - None
     */
    public Transaction resolveDeadlock(List<Transaction> transactions) {
        Transaction earliestTransaction = null;
        int earliestTime = Integer.MIN_VALUE;

        for(Transaction t : transactions) {
            for(int node : affectedNodes) {
                if(t.transaction_id == node && t.begin_time > earliestTime) {
                    earliestTime = t.begin_time;
                    earliestTransaction = t;
                }
            }
        }
        removeEdge(earliestTransaction.transaction_id);
        return earliestTransaction;
    }

    /**
     * dfs function to check for cycle in the graph
     *
     * @param index
     * @param visited
     * @param recursionStack
     * @return boolean
     * @sideEffects - None
     */
    private boolean dfs(int index, boolean[] visited, boolean[] recursionStack) {
        if (recursionStack[index]) {
            return true;
        }
        if (visited[index]) {
            return false;
        }

        recursionStack[index] = true;

        List<Integer> neighbors = adj.get(index);

        for (Integer n : neighbors) {
            if (dfs(n, visited, recursionStack)) {
                affectedNodes.add(n);
                return true;
            }
        }
        recursionStack[index] = false;

        return false;
    }
}
