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
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.InvalidCredentialsException;


public class OnlineUpdate {
    private static final Logger logger = Logger.getLogger(OnlineUpdate.class);
    
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
        logger.info("Elapsed time: " + elapsedTime + " s");
        
       	System.exit(exitCode);
   }
	
	private int update(String fileName, String password, String quoteUrl) {
		
		final int goodExit = 0;
		final int warnExit = 1; 
		final int errorExit = 2;
				
		// Get quote data from Yahoo API
		JsonNode quotesJson = getQuotesJson(quoteUrl);
		if (quotesJson == null) {
			return errorExit;
		}
		
		// Open Money database
		Database openDb = null;
		try {
			openDb = MnyDb.open(fileName, password);
		} catch (IOException | InvalidCredentialsException e) {
			logger.fatal(e.getMessage());
			return errorExit;
		}		
				
		// Process quote data
		boolean exitFlag = true;
		int exitCode = goodExit;
						
		//ArrayNode resultAn = (ArrayNode) quotesJson.at("/quoteResponse/result");
		//Iterator<JsonNode> resultIt = resultAn.elements();
		Iterator<JsonNode> resultIt = quotesJson.at("/quoteResponse/result").elements();
		while (resultIt.hasNext()) {
			JsonNode result = resultIt.next(); 
			try {
				if (result.get("quoteType").asText().equals("CURRENCY")) {
					exitFlag = updateFxRow(openDb, result);
				} else {
					exitFlag = updateSpRow(openDb, result);
				}
			} catch (Exception e) {
				// Something unexpected has gone wrong so get the stack trace
				e.printStackTrace();
				exitCode = errorExit;
			}
			if (!exitFlag && exitCode == goodExit) {
				exitCode = warnExit;
			}
		}
				
		// Close Money database
	    try {
	    	MnyDb.close(openDb);
	    } catch (IOException e) {
	    	logger.error(e);
	    	return errorExit;
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
    		logger.fatal(e);
    	}
		return quotesJson;
    }

    private boolean updateSpRow(Database db, JsonNode quote) throws IOException {
            	
        String quoteType = quote.get("quoteType").asText();
        String symbol = quote.get("symbol").asText();
                
    	// Find matching symbol in SEC table
        Map<String, Object> secRow = null;
        Map<String, Object> secRowPattern = new HashMap<String, Object>();
        logger.info("Processing quote for symbol " + symbol + ", quote type = " + quoteType);
    	int hsec = -1;
    	Table secTable = db.getTable("SEC");
    	Cursor secCursor = CursorBuilder.createCursor(secTable);
    	
        secRowPattern.put("szSymbol", symbol);
    	if (secCursor.findFirstRow(secRowPattern)) {
            secRow = secCursor.getCurrentRow();
            hsec = (int) secRow.get("hsec");
        }
    		
    	if (hsec == -1) {
    		logger.warn("Cannot find symbol " + symbol);
    		return false;
	   	}

    	logger.info("Found symbol " + symbol + ", sct = " + secRow.get("sct") + ", hsec = " + hsec);
    	
    	// Get quote date and adjust for local system time-zone offset
    	// Note: Jackcess still uses Date objects
    	Instant quoteInstant = Instant.ofEpochSecond(quote.get("regularMarketTime").asLong());
    	ZoneOffset quoteZoneOffset = ZoneId.systemDefault().getRules().getOffset(quoteInstant);
    	int offsetSeconds = quoteZoneOffset.getTotalSeconds();
    	Date quoteDate = Date.from(quoteInstant.truncatedTo(ChronoUnit.DAYS).minusSeconds(offsetSeconds));
    	
    	// Find matching symbol and quote date in SP table
    	Table spTable = db.getTable("SP");
    	Cursor spCursor = CursorBuilder.createCursor(spTable);
    	Map<String, Object> spRow = null;
        Map<String, Object> spRowPattern = new HashMap<String, Object>();
    	boolean needNewSpRow = true;
    	spRowPattern.put("hsec", hsec);
    	spRowPattern.put("dt", quoteDate);
    	double oldPrice = 0;
    	Date oldDate = null;
    	// src: manual update = 5, online update = 6
    	int[] srcs = { 5, 6 };
        for (int src : srcs) {
        	spRowPattern.put("src", src);
               	if (spCursor.findFirstRow(spRowPattern)) {
               		// Matching SP row found
               		spRow = spCursor.getCurrentRow();
               		oldPrice = (double) spRow.get("dPrice");
               		oldDate = (Date) spRow.get("dt");
               		needNewSpRow = false;
               		break;
        	}
    	}
       	if (needNewSpRow) {
    		// No matching SP row found - build new row
    		spRow = buildNewSpRow(spCursor, hsec, quoteDate);
    		oldDate = (Date) spRow.get("dt");
    		oldPrice = (double) spRow.get("dPrice");
    		// Index autonumber test:
    		// spRow.put("hsp", null);
    		spRow.put("dt", quoteDate);
    		spRow.put("src", 6);
       	}

       	if (oldPrice == -1) {
       		logger.info("Cannot find previous quote for symbol " + symbol);
       	}
       	else {
       		logger.info("Found previous quote for symbol " + symbol + ": " + oldDate + ", price = " + oldPrice);
       	}
       	
       	// Set price factor
       	double priceFactor = 1;
       	int yieldFactor = 100;
                
        if (quoteType.equals("EQUITY") || quoteType.equals("BOND")) {
        	if (quote.get("currency").asText().toUpperCase().equals("GBP")) {
        		priceFactor = 0.01;
        		yieldFactor = 10000;
        	} 
        }
    	
        // Build SEC and SP rows from JSON values
        double dPrice = quote.get("regularMarketPrice").asDouble() * priceFactor;
        
        // dtSerial is assumed to be record creation/update time-stamp
        Date dateSerial = new Date();
        
        // SP fields common to EQUITY, BOND, MUTUALFUND and INDEX quote types
        spRow.put("dtSerial", dateSerial);
	    spRow.put("dPrice", dPrice);
	    spRow.put("dChange", quote.get("regularMarketChange").asDouble() * priceFactor);
		
	    // SEC fields common to EQUITY, BOND, MUTUALFUND and INDEX quote types
	    secRow.put("dtSerial", dateSerial);
        secRow.put("dtLastUpdate", quoteDate);
        secRow.put("d52WeekLow", quote.get("fiftyTwoWeekLow").asDouble() * priceFactor);
        secRow.put("d52WeekHigh", quote.get("fiftyTwoWeekHigh").asDouble() * priceFactor);
	    	    
        // Fields common to EQUITY, BOND and INDEX quote types
	    if (quoteType.equals("EQUITY") || quoteType.equals("BOND") || quoteType.equals("INDEX")) {
		    // SP fields
	    	spRow.put("dOpen", quote.get("regularMarketOpen").asDouble() * priceFactor);
		    spRow.put("dHigh", quote.get("regularMarketDayHigh").asDouble() * priceFactor);
		    spRow.put("dLow", quote.get("regularMarketDayLow").asDouble() * priceFactor);
		    spRow.put("vol", quote.get("regularMarketVolume").asLong());
		    // SEC fields
	        secRow.put("dBid", quote.get("bid").asDouble() * priceFactor);
	        secRow.put("dAsk", quote.get("ask").asDouble() * priceFactor);
	    }
        
        // EQUITY only fields
        // TODO add EPS and beta
        if (quoteType.equals("EQUITY")) {
        	secRow.put("dCapitalization", quote.get("marketCap").asDouble());
            secRow.put("dSharesOutstanding", quote.get("sharesOutstanding").asDouble());
            if (quote.has("trailingPE")) {
            	spRow.put("dPE", quote.get("trailingPE").asDouble());
           }
            if (quote.has("trailingAnnualDividendYield")) {
            	secRow.put("dDividendYield", quote.get("trailingAnnualDividendYield").asDouble() * yieldFactor);
            } else {
        		secRow.put("dDividendYield", 0);
        	}
        }
        
        // Update SEC and SP tables
        secCursor.updateCurrentRowFromMap(secRow);
       	
        if (needNewSpRow) {
        	spTable.addRowFromMap(spRow);
        	//spTable.addRow(spRow.values().toArray());
        	logger.info("Added new quote for symbol " + symbol + ": " + quoteDate + ", new price = " + dPrice + ", new hsp = " + spRow.get("hsp"));
        } else {
        	spCursor.updateCurrentRowFromMap(spRow);
        	//spCursor.updateCurrentRow(spRow.values().toArray());
            logger.info("Updated previous quote for symbol " + symbol + ": " + quoteDate + ", new price = " + dPrice + ", hsp = " + spRow.get("hsp"));
    	}        
        
        // Done
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
				if ((src == 5 || src == 6) && rowDate.before(quoteDate)) {
		        	maxDate = rowDate;
		        	newRow = row;
			    }
				// Test for previous buy
		        if (src == 1 && (rowDate.before(quoteDate) || rowDate.equals(quoteDate))) {
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
    	logger.info("Processing quote for currency pair " + currencyPair);
    
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
                logger.info("Found currency " + isoCode + ", hcrnc = " + hcrncs[n]);
            } else {
            	logger.warn("Cannot find currency " + isoCode);
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
        		logger.warn("Cannot find previous rate for currency pair " + currencyPair);
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
                logger.info("Found currency pair: from hcrnc = " + hcrncs[n] + ", to hcrnc = " + hcrncs[(n + 1) % 2]);
                Column column = rateTable.getColumn("rate");
                rateCursor.setCurrentRowValue(column, newRate);
                logger.info("Updated currency pair " + currencyPair + ": previous rate = " + oldRate + ", new rate = " + newRate);
                return true;
            }	
        }
    return true;
    }
    
}