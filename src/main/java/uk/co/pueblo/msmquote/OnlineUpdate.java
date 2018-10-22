package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.InvalidCredentialsException;


public class OnlineUpdate {
        
    // Set quote-type constants
    private static final String QT_EQUITY = "EQUITY";
    private static final String QT_BOND = "BOND";
    private static final String QT_MF = "MUTUALFUND";
    private static final String QT_INDEX = "INDEX";
    private static final String QT_CURRENCY = "CURRENCY";
    
    // Set SP table src constants
    private static final int SRC_BUY = 1;
    private static final int SRC_MANUAL = 5;
    private static final int SRC_ONLINE = 6;
	
    // Set exit code constants
    private static final int EXIT_OK = 0;
    private static final int EXIT_WARNING = 1;
    private static final int EXIT_ERROR = 2;
    
    // Set miscellaneous constants
    private static final Logger LOGGER = Logger.getLogger(OnlineUpdate.class);
    
    public static void main(String[] args) {
    	
    	double startTime = Instant.now().toEpochMilli();
		
		// Get arguments
    	String password = null;
    	String url = null;
	    
    	if (args.length == 3) {
	    	password = args[1];
	    	url = args[2];
    	} else if (args.length == 2) {
    		url = args[1];
    	} else {
	    	System.out.printf("Usage: %s filename [password] URL", OnlineUpdate.class.getName());
	        return;
	    }

    	int exitCode = 0; 
	    OnlineUpdate doUpdate = new OnlineUpdate();
        exitCode = (doUpdate.update(args[0], password, url));
        
        double elapsedTime = (Instant.now().toEpochMilli() - startTime) / 1000;
        LOGGER.info("Elapsed time: " + elapsedTime + " s");
        
       	System.exit(exitCode);
   }
	
	private int update(String fileName, String password, String quoteUrl) {
						
		// Get quote data from Yahoo API
		JsonNode quotesJson = getQuotesJson(quoteUrl);
		if (quotesJson == null) {
			return EXIT_ERROR;
		}
		
		// Open Money database
		Database msmDb = null;
		try {
			msmDb = MsmUtils.openDb(fileName, password);
		} catch (IOException | InvalidCredentialsException e) {
			LOGGER.fatal(e.getMessage());
			return EXIT_ERROR;
		}		
				
		// Process quote data
		int exitCode = EXIT_OK;
						
		//ArrayNode resultAn = (ArrayNode) quotesJson.at("/quoteResponse/result");
		//Iterator<JsonNode> resultIt = resultAn.elements();
		Iterator<JsonNode> resultIt = quotesJson.at("/quoteResponse/result").elements();
		while (resultIt.hasNext()) {
			JsonNode result = resultIt.next();
			
			// Get quote type
			String symbol = result.get("symbol").asText();
			String quoteType = result.get("quoteType").asText();			
			LOGGER.info("Processing quote data for symbol " + symbol + ", quote type = " + quoteType);
			
			// Get quote date and adjust for local system time-zone offset
	    	// Note: Jackcess still uses Date objects
	    	Instant quoteInstant = Instant.ofEpochSecond(result.get("regularMarketTime").asLong());
	    	ZoneOffset quoteZoneOffset = ZoneId.systemDefault().getRules().getOffset(quoteInstant);
	    	int offsetSeconds = quoteZoneOffset.getTotalSeconds();
	    	Date quoteDate = Date.from(quoteInstant.truncatedTo(ChronoUnit.DAYS).minusSeconds(offsetSeconds));
	    	
	    	// Set quote factor
	    	double quoteFactor = 1;
	    	if (quoteType.equals(QT_EQUITY) || quoteType.equals(QT_BOND)) {
	        	if (result.get("currency").asText().toUpperCase().equals("GBP")) {
	        		quoteFactor = 0.01;
	        	} 
	        }
		
	    	// Truncate symbol to maximum Money symbol length of 12 characters
	    	String origSymbol = symbol;
	        if (symbol.length() > 12) {
	    		symbol = symbol.substring(0, 12);
	    		LOGGER.info("Truncated symbol " + origSymbol + " to " + symbol);
	    	}	    	
	    	
	    	// Update tables in Money database
	    	try {
				if (quoteType.equals(QT_CURRENCY)) {
					if (updateFxRow(msmDb, result) == false) {
						exitCode = EXIT_WARNING;
					}
				} else {
					int hsec = updateSecRow(msmDb, result, symbol, quoteType, quoteDate, quoteFactor);
					if (hsec == -1) {
						exitCode = EXIT_WARNING;
					} else {
						if(updateSpRow(msmDb, result, symbol, quoteType, quoteDate, quoteFactor, hsec) == false);
							exitCode = EXIT_WARNING;
					}
				}
			} catch (Exception e) {
				// Something unexpected has gone wrong so get the stack trace
				e.printStackTrace();
				exitCode = EXIT_ERROR;
			}
		}
				
		// Close Money database
	    try {
	    	MsmUtils.closeDb(msmDb);
	    } catch (IOException e) {
	    	LOGGER.error(e);
	    	return EXIT_ERROR;
	    }
	    	    
	return exitCode;
	}

	private JsonNode getQuotesJson(String quoteUrl) {
    	JsonNode quotesJson = null;
    	// Using try-with-resources to get AutoClose of InputStream
    	try (InputStream quoteResp = new URL(quoteUrl).openStream();) {
			ObjectMapper mapper = new ObjectMapper();
			quotesJson = mapper.readTree(quoteResp);
    	} catch (IOException e) {
    		LOGGER.fatal(e);
    	}
		return quotesJson;
    }
	
	private int updateSecRow(Database db, JsonNode quote, String symbol, String quoteType, Date quoteDate, double quoteFactor) throws IOException {
		
    	// Find matching symbol in SEC table
        Map<String, Object> row = null;
        Map<String, Object> rowPattern = new HashMap<String, Object>();
        int hsec = -1;
    	Table table = db.getTable("SEC");
    	IndexCursor cursor = CursorBuilder.createCursor(table.getPrimaryKeyIndex());
    	
        rowPattern.put("szSymbol", symbol);
    	if (cursor.findFirstRow(rowPattern)) {
            row = cursor.getCurrentRow();
            hsec = (int) row.get("hsec");
        }
    		
    	if (cursor.isBeforeFirst()) {
    		LOGGER.warn("Cannot find symbol " + symbol + " in SEC table");
    		return hsec;
	   	}

    	LOGGER.info("Found symbol " + symbol + " in SEC table: sct = " + row.get("sct") + ", hsec = " + hsec);
    	
    	// Build SEC fields common to EQUITY, BOND, MUTUALFUND and INDEX quote types
    	// dtSerial is assumed to be record creation/update time-stamp
	    row.put("dtSerial", new Date());
        row.put("dtLastUpdate", quoteDate);
        row.put("d52WeekLow", quote.get("fiftyTwoWeekLow").asDouble() * quoteFactor);
        row.put("d52WeekHigh", quote.get("fiftyTwoWeekHigh").asDouble() * quoteFactor);
	    	    
        // Build SEC fields common to EQUITY, BOND and INDEX quote types
	    if (quoteType.equals(QT_EQUITY) || quoteType.equals(QT_BOND) || quoteType.equals(QT_INDEX)) {
		    row.put("dBid", quote.get("bid").asDouble() * quoteFactor);
	        row.put("dAsk", quote.get("ask").asDouble() * quoteFactor);
	    }
	    
	    // Build EQUITY-only SEC fields
        // TODO add EPS and beta
        if (quoteType.equals("EQUITY")) {
        	row.put("dCapitalization", quote.get("marketCap").asDouble());
            row.put("dSharesOutstanding", quote.get("sharesOutstanding").asDouble());
            if (quote.has("trailingAnnualDividendYield")) {
            	row.put("dDividendYield", quote.get("trailingAnnualDividendYield").asDouble() * 100 / quoteFactor);
            } else {
        		row.put("dDividendYield", 0);
        	}
        }
        
        // Update SEC table
        cursor.updateCurrentRowFromMap(row);
        LOGGER.info("Updated quote data in SEC table for symbol " + symbol);

	return hsec;
	}

    private boolean updateSpRow(Database db, JsonNode quote, String symbol, String quoteType, Date quoteDate, double quoteFactor, int hsec) throws IOException {
    	
    	if (quote.get("regularMarketTime").asLong() == 0) {
    		LOGGER.warn("Skipped SP table update for symbol " + symbol + ", quote date = " + quoteDate);
    		return false;
    	}
           	
    	// Find matching symbol and quote date in SP table
    	Table table = db.getTable("SP");
    	IndexCursor cursor = CursorBuilder.createCursor(table.getPrimaryKeyIndex());
    	Map<String, Object> row = null;
        Map<String, Object> rowPattern = new HashMap<String, Object>();
    	boolean needNewSpRow = true;
    	rowPattern.put("hsec", hsec);
    	rowPattern.put("dt", quoteDate);
    	double oldPrice = 0;
    	Date oldDate = null;
    	int[] srcs = { SRC_MANUAL, SRC_ONLINE };

    	for (int src : srcs) {
        	rowPattern.put("src", src);
               	if (cursor.findFirstRow(rowPattern)) {
               		// Matching SP row found
               		row = cursor.getCurrentRow();
               		oldPrice = (double) row.get("dPrice");
               		oldDate = (Date) row.get("dt");
               		needNewSpRow = false;
               		break;
        	}
    	}
       	if (needNewSpRow) {
    		// No matching SP row found - build new row
    		row = buildNewSpRow(cursor, hsec, quoteDate);
    		oldDate = (Date) row.get("dt");
    		oldPrice = (double) row.get("dPrice");
    		// Index autonumber test:
    		// spRow.put("hsp", null);
    		row.put("dt", quoteDate);
    		row.put("src", SRC_ONLINE);
       	}

       	
       	
       	if (oldPrice == -1) {
       		LOGGER.info("Cannot find previous quote in SP table for symbol " + symbol);
       	}
       	else {
       		LOGGER.info("Found previous quote in SP table for symbol " + symbol + ": " + oldDate + ", price = " + oldPrice);
       	}
       	
        double dPrice = quote.get("regularMarketPrice").asDouble() * quoteFactor;
                
        // Build SP fields common to EQUITY, BOND, MUTUALFUND and INDEX quote types
        // dtSerial is assumed to be record creation/update time-stamp
        row.put("dtSerial", new Date());
	    row.put("dPrice", dPrice);
	    row.put("dChange", quote.get("regularMarketChange").asDouble() * quoteFactor);
	    	    
        // Build SP fields common to EQUITY, BOND and INDEX quote types
	    if (quoteType.equals(QT_EQUITY) || quoteType.equals(QT_BOND) || quoteType.equals(QT_INDEX)) {
	    	row.put("dOpen", quote.get("regularMarketOpen").asDouble() * quoteFactor);
		    row.put("dHigh", quote.get("regularMarketDayHigh").asDouble() * quoteFactor);
		    row.put("dLow", quote.get("regularMarketDayLow").asDouble() * quoteFactor);
		    row.put("vol", quote.get("regularMarketVolume").asLong());
	    }
        
        // Build EQUITY-only SP fields
        // TODO add EPS and beta
        if (quoteType.equals(QT_EQUITY)) {
            if (quote.has("trailingPE")) {
            	row.put("dPE", quote.get("trailingPE").asDouble());
           }
        }
        
        // Update SP row
        if (needNewSpRow) {
        	table.addRowFromMap(row);
           	LOGGER.info("Added new quote data in SP table for symbol " + symbol + ": " + quoteDate + ", new price = " + dPrice + ", new hsp = " + row.get("hsp"));
        } else {
        	cursor.updateCurrentRowFromMap(row);
            LOGGER.info("Updated previous quote data in SP table for symbol " + symbol + ": " + quoteDate + ", new price = " + dPrice + ", hsp = " + row.get("hsp"));
    	}        
        
        return true;
    }
    
    /** 
     * Build a new SP table row.
     * 
     * Returns a row containing the previous price and date and the next hsp (index).
     * 
     */
    private Map<String, Object> buildNewSpRow(Cursor cursor, int hsec, Date quoteDate) throws IOException {
    	Map<String, Object> row = null;
    	Map<String, Object> newRow = new HashMap<String, Object>();
    	int rowHsp = 0;
    	int maxHsp = 0;
    	Date rowDate = new Date(0);
    	Date maxDate = new Date(0);
    	
    	// Build minimal new row
    	newRow.put("hsec", hsec);
    	newRow.put("dPrice", (double) -1);
    	    	
    	// Find highest hsp and any previous quotes
       	cursor.beforeFirst();
    	while (true) {
    		row = cursor.getNextRow();
    		if (row == null) {
    			break;
    		}
    		rowHsp = (int) row.get("hsp");
    		if (rowHsp > maxHsp) {
    			maxHsp = rowHsp;
    		}
    		// src: buy = 1, manual update = 5, online update = 6
			int src = (int) row.get("src");
			rowDate = (Date) row.get("dt");
			if (((int) row.get("hsec") == hsec) && rowDate.after(maxDate)) {
				// Test for previous manual or online quote
				if ((src == SRC_MANUAL || src == SRC_ONLINE) && rowDate.before(quoteDate)) {
		        	maxDate = rowDate;
		        	newRow = row;
			    }
				// Test for previous buy
		        if (src == SRC_BUY && (rowDate.before(quoteDate) || rowDate.equals(quoteDate))) {
	        		maxDate = rowDate;
		        	newRow = row;
	        	}
	        }
    	}
			
    newRow.put("hsp", maxHsp + 1);
	return newRow;
    }
	
    private boolean updateFxRow(Database db, JsonNode quote) throws IOException {
    	String currencyPair = quote.get("symbol").asText();
    	LOGGER.info("Processing quote for currency pair " + currencyPair);
    
        int n = 0;
    	
        // Find currencies in CRNC table
        Table crncTable = db.getTable("CRNC");
        Cursor crncCursor = CursorBuilder.createCursor(crncTable);
        Map<String, Object> crncRowPattern = new HashMap<String, Object>();
        Map<String, Object> crncRow = null;
        String isoCode = null;
        int[] hcrncs = {0, 0};
            
        for (n = 0; n < 2; n++) {
        	isoCode = currencyPair.substring(n * 3, (n + 1) * 3); 
        	crncRowPattern.put("szIsoCode", isoCode);
            if (crncCursor.findFirstRow(crncRowPattern)) {
                crncRow = crncCursor.getCurrentRow();
                hcrncs[n] = (int) crncRow.get("hcrnc");
                LOGGER.info("Found currency " + isoCode + ", hcrnc = " + hcrncs[n]);
            } else {
            	LOGGER.warn("Cannot find currency " + isoCode);
        		return false;
    	   	}
        }	
                                      
        // Now find and update rate for currency pair in CRNC_EXCHG table
        Table rateTable = db.getTable("CRNC_EXCHG");                        
        Cursor rateCursor = CursorBuilder.createCursor(rateTable);
        Map<String, Object> rateRowPattern = new HashMap<String, Object>();
        Map<String, Object> rateRow = null;
        double oldRate = 0;
        double newRate = quote.get("regularMarketPrice").asDouble();

        for (n = 0; n < 3; n++) {
        	if (n == 2) {
        		LOGGER.warn("Cannot find previous rate for currency pair " + currencyPair);
        		return false;
        	}
           	rateRowPattern.put("hcrncFrom", hcrncs[n]);
            rateRowPattern.put("hcrncTo", hcrncs[(n + 1) % 2]);
            if (rateCursor.findFirstRow(rateRowPattern)) {
                rateRow = rateCursor.getCurrentRow();
                oldRate = (double) rateRow.get("rate");
                if (n == 1) {
                	// Reversed rate
                	newRate = 1 / newRate;                	
                }
                LOGGER.info("Found currency pair: from hcrnc = " + hcrncs[n] + ", to hcrnc = " + hcrncs[(n + 1) % 2]);
                Column column = rateTable.getColumn("rate");
                rateCursor.setCurrentRowValue(column, newRate);
                LOGGER.info("Updated currency pair " + currencyPair + ": previous rate = " + oldRate + ", new rate = " + newRate);
                return true;
            }	
        }
    return true;
    }
    
}