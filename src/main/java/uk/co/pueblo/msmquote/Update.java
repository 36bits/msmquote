package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Duration;
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
    	
    	Instant startTime = Instant.now();
		
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
    	
    	// Open Money database
    	MsmDb msmDb = null;
    	Database openMsmDb = null;
		try {
			msmDb = new MsmDb(args[0], password);
			openMsmDb = msmDb.getDb();
		} catch (Exception e) {
			LOGGER.fatal(e);
			System.exit(ERROR);
		}
    	
		// Process quote data
		Map<String, Object> quoteRow = new HashMap<>();
		MsmTables msmTables = null;
		
		try {
			msmTables = new MsmTables(openMsmDb);
			YahooQuote yahooQuote = new YahooQuote(source);
			while (true) {
				if ((quoteRow = yahooQuote.getNext()) == null) {
					break;
				}
				if (!msmTables.update(quoteRow)) {
					exitCode = WARNING;
				}
				//quoteRow.clear();
			}
		} catch (IOException e) {
			LOGGER.fatal(e);
			exitCode = ERROR;
		} finally {
			// Add any new rows to the SP table and close the Money database
			try {
				msmTables.addNewSpRows();
				msmDb.closeDb();
			} catch (IOException e) {
				LOGGER.fatal(e);
				exitCode = ERROR;
			}
		}
					
        LOGGER.info("Duration: " + Duration.between(startTime, Instant.now()).toString());
       	System.exit(exitCode);
   }
}