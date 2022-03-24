/**
 Main.cpp
 Purpose: The main function
 @author Niharika Sinha (ns4451)
 */
import TransactionManager.TransactionManager;
import IO.IO;

public class Main {

    public static void main(String []args) {
        TransactionManager tm = new TransactionManager();
        tm.site_init();

        String filename = args[0];
        IO io = new IO(filename);

        tm.runSystem(io);

    }
}
