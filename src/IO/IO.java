/**
 IO.java
 Purpose: To parse the input file and handle print statements
 @author Niharika Sinha (ns4451)
 */

package IO;
import Site.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import Transaction.*;




public class IO {
    public BufferedReader rd;

    /**
     * Prints the value read by a transaction id
     *
     * @param t, Transaction object
     * @param variable, value
     * @return void
     * @sideEffects - None
     */
    public void printReadValue(Transaction t, String variable, int value) {
        System.out.println("Reading "+variable+" -> "+value+" by T"+t.transaction_id);
    }

    /**
     * Print the line
     *
     * @param lineToPrint
     * @return void
     * @sideEffects - None
     */
    public void print(String lineToPrint) {
        System.out.println(lineToPrint);
    }

    public IO() {

    }

    /**
     * Create reader object for input file
     *
     * @param filename
     * @return void
     * @sideEffects - None
     */
    public IO(String filename){
        try{
            rd = new BufferedReader(new FileReader(filename));
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Read a line from the input file
     *
     * @param void
     * @return String
     * @sideEffects - None
     */
    public String readLine(){
        String line = "";
        try{
            line = rd.readLine();
        }catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }
}

