package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

public class Update {
    private static final Logger LOGGER = LogManager.getLogger(Update.class);
	
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
    	LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());
    	
    	// Open Money database
    	MsmDb db = null;
    	Database openedDb = null;
		try {
			db = new MsmDb(args[0], password);
			openedDb = db.getDb();
		} catch (Exception e) {
			LOGGER.fatal(e);
			System.exit(ERROR);
		}
    	
		// Process quote data
		int hsec;
		Map<String, Object> quoteRow = new HashMap<>();
		MsmSecTable secTable = null;
		MsmSpTable spTable = null;
		MsmFxTable fxTable = null;
		
		try {
			secTable = new MsmSecTable(openedDb);
			spTable = new MsmSpTable(openedDb);
			fxTable = new MsmFxTable(openedDb);
			YahooQuote yahooQuote = new YahooQuote(source);
			while (true) {
				if ((quoteRow = yahooQuote.getNext()) == null) {
					break;
				}
				if (quoteRow.get("rate") == null) {
					if((hsec = secTable.update(quoteRow)) != -1) {
						spTable.update(quoteRow, hsec);
					} else {
						exitCode = WARNING;
					}
				} else {		
					if (!fxTable.update(quoteRow)) {
						exitCode = WARNING;
					}
				}
			}
		} catch (IOException e) {
			LOGGER.fatal(e);
			exitCode = ERROR;
		} finally {
			// Add any new rows to the SP table and close the Money database
			try {
				spTable.addNewRows();
				db.closeDb();
			} catch (IOException e) {
				LOGGER.fatal(e);
				exitCode = ERROR;
			}
		}
					
        LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
       	System.exit(exitCode);
   }
}