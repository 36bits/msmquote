package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

public class Update {
    private static final Logger LOGGER = Logger.getLogger(Update.class);
	
    // Define exit code fields
    private static final int OK = 0;
    private static final int WARNING = 1;
    private static final int ERROR = 2;
        
    public static void main(String[] args) {
    	
    	double startTime = Instant.now().toEpochMilli();
		
		// Get arguments
    	String password = null;
    	String source = null;
	    
    	if (args.length == 3) {
	    	password = args[1];
	    	source = args[2];
    	} else if (args.length == 2) {
    		source = args[1];
    	} else {
	    	System.out.printf("Usage: %s filename [password] source", Update.class.getName());
	        return;
	    }    	

    	int exitCode = OK; 
    	
    	// Open Money database and get quote data from Yahoo API
    	MsmDb msmDb = null;
    	Database openMsmDb = null;
    	MsmTables msmTables = null;
    	YahooQuote yahooQuote = null;
		try {
			msmDb = new MsmDb(args[0], password);
			openMsmDb = msmDb.getDb();
			msmTables = new MsmTables(openMsmDb);
			yahooQuote = new YahooQuote(source);
		} catch (Exception e) {
			LOGGER.fatal(e);
			System.exit(ERROR);
		}
    	
		// Process quota data
		Map<String, Object> quoteRow = new HashMap<String, Object>();
		
		try {
			while (true) {
				quoteRow = yahooQuote.getNext();
				if (quoteRow == null) {
					break;
				}
				if (!msmTables.update(quoteRow)) {
					exitCode = WARNING;
				}
				quoteRow.clear();
			}
		} catch (IOException e) {
			LOGGER.fatal(e);
			exitCode = ERROR;
		} finally {
			// Close Money database
			try {
				msmDb.closeDb();
			} catch (IOException e) {
				LOGGER.fatal(e);
				exitCode = ERROR;
			}
		}
					
        double elapsedTime = (Instant.now().toEpochMilli() - startTime) / 1000;
        LOGGER.info("Elapsed time: " + elapsedTime + " s");
        
       	System.exit(exitCode);
   }
}