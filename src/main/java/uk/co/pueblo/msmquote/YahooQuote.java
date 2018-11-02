package uk.co.pueblo.msmquote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class YahooQuote {
	
	private static final Logger LOGGER = LogManager.getLogger(YahooQuote.class);
	
	// Set Yahoo quote-type fields
    private static final String EQUITY = "EQUITY";
    private static final String BOND = "BOND";
    private static final String MF = "MUTUALFUND";
    private static final String INDEX = "INDEX";
    private static final String CURRENCY = "CURRENCY";
    
    // Set quote format fields
    private static final int UNKNOWN = 0;
    private static final int JSON = 1;
    private static final int CSV = 2;
    
	private ZoneId sysZoneId = ZoneId.systemDefault();
    private Iterator<JsonNode> resultIt;
	private BufferedReader csvBr;
	private String symbol;
	private int quoteFormat = UNKNOWN;
	private double quoteFactor = 1;
				
	// Constructor
	public YahooQuote(String source) throws IOException {
		
		if (source.startsWith("https://") || source.startsWith("http://")) {
			// Source type is API
			JsonNode quotesJson = null;
		  	// Using try-with-resources to get AutoClose of InputStream
			LOGGER.info("Requesting quote data from Yahoo API");
			try (InputStream quoteIs = new URL(source).openStream();) {
				ObjectMapper mapper = new ObjectMapper();
				quotesJson = mapper.readTree(quoteIs);
	    	} 
	    	resultIt = quotesJson.at("/quoteResponse/result").elements();
	    	quoteFormat = JSON;
		} else {
			// Assume source is CSV file
			File csvFile = new File(source);
			csvFile = new File(source);
			csvBr = new BufferedReader(new FileReader(csvFile));
			if (!csvBr.readLine().equals("Date,Open,High,Low,Close,Adj Close,Volume")) {
				LOGGER.warn("Yahoo CSV header not found in file {}", source);
				csvBr.close();
			}				
			
			// Get quote symbol from CSV file name & truncate if required
			symbol = csvFile.getName();
			if (symbol.toLowerCase().endsWith(".csv")) {
				symbol = symbol.substring(0, symbol.length() - 4);	
			}
			symbol = truncateSymbol(symbol);
			
			// Set quote factor for GB quotes
			if (symbol.toUpperCase().endsWith(".L")) {
				quoteFactor = 0.01;
			}
			quoteFormat = CSV;
		}
		return;
	}
	
	public Map<String, Object> getNext() throws IOException {
				
		switch (quoteFormat) {
			case JSON:	return getNextJsonQuote();
			case CSV:	return getNextCsvQuote();	
		}
		return null;
	}
		
	private Map<String, Object> getNextJsonQuote() {
		
		// Get next JSON node from iterator
		if (!resultIt.hasNext()) {
			return null;
		}
		JsonNode result = resultIt.next();
		
		Map<String, Object> quoteRow = new HashMap<>();
				
		// Get quote type
		String symbol = result.get("symbol").asText();
		String quoteType = result.get("quoteType").asText();			
		LOGGER.info("Processing quote data for symbol {}, quote type = {}", symbol, quoteType);
						    	
		if (quoteType.equals(CURRENCY)) {
			quoteRow.put("symbol", result.get("symbol").asText());
			quoteRow.put("rate", result.get("regularMarketPrice").asDouble());
		} else {
			// Truncate symbol to maximum Money symbol length of 12 characters
			symbol = truncateSymbol(symbol);
			
			// Get quote date and adjust for local system time-zone offset
	    	// Note: Jackcess still uses Date objects
	        /* 
	    	Instant quoteInstant = Instant.ofEpochSecond(result.get("regularMarketTime").asLong());
	    	ZoneOffset quoteZoneOffset = ZoneId.systemDefault().getRules().getOffset(quoteInstant);
	    	int offsetSeconds = quoteZoneOffset.getTotalSeconds();
	    	Date quoteDate = null;
	    	quoteDate = Date.from(quoteInstant.truncatedTo(ChronoUnit.DAYS).minusSeconds(offsetSeconds));
	    	*/
	    	
	        LocalDate quoteLd = Instant.ofEpochSecond(result.get("regularMarketTime").asLong()).atZone(sysZoneId).toLocalDate();
			Date quoteDate = Date.from(quoteLd.atStartOfDay(sysZoneId).toInstant());
	    		    	
	    	// Set quote factor for GB quotes
	    	double quoteFactor = 1;
	    	if (quoteType.equals(EQUITY) || quoteType.equals(BOND)) {
	        	if (result.get("currency").asText().toUpperCase().equals("GBP")) {
	        		quoteFactor = 0.01;
	        	} 
	        }
	    				    	
	    	// Build quote row
	    	// Columns common to EQUITY, BOND, MUTUALFUND and INDEX quote types
			// SEC table
	    	quoteRow.put("szSymbol", symbol);
			// Assume dtLastUpdate is date of quote data in SEC row
			quoteRow.put("dtLastUpdate", quoteDate);
	        quoteRow.put("d52WeekLow", result.get("fiftyTwoWeekLow").asDouble() * quoteFactor);
	        quoteRow.put("d52WeekHigh", result.get("fiftyTwoWeekHigh").asDouble() * quoteFactor);
		    // SP table
	        quoteRow.put("dt", quoteDate);
			quoteRow.put("dPrice", result.get("regularMarketPrice").asDouble() * quoteFactor);
		    quoteRow.put("dChange", result.get("regularMarketChange").asDouble() * quoteFactor);
	        	        
	        // Columns common to EQUITY, BOND and INDEX quote types
		    if (quoteType.equals(EQUITY) || quoteType.equals(BOND) || quoteType.equals(INDEX)) {
		    	// SEC table
			    quoteRow.put("dBid", result.get("bid").asDouble() * quoteFactor);
		        quoteRow.put("dAsk", result.get("ask").asDouble() * quoteFactor);
		        // SP table
		        quoteRow.put("dOpen", result.get("regularMarketOpen").asDouble() * quoteFactor);
			    quoteRow.put("dHigh", result.get("regularMarketDayHigh").asDouble() * quoteFactor);
			    quoteRow.put("dLow", result.get("regularMarketDayLow").asDouble() * quoteFactor);
			    quoteRow.put("vol", result.get("regularMarketVolume").asLong());
		    }
		    
		    // Columns for EQUITY quote types
	       if (quoteType.equals("EQUITY")) {
	        	// SEC table
		        // TODO Add EPS and beta
	        	quoteRow.put("dCapitalization", result.get("marketCap").asDouble());
	            quoteRow.put("dSharesOutstanding", result.get("sharesOutstanding").asDouble());
	            if (result.has("trailingAnnualDividendYield")) {
	            	quoteRow.put("dDividendYield", result.get("trailingAnnualDividendYield").asDouble() * 100 / quoteFactor);
	            } else {
	        		quoteRow.put("dDividendYield", 0);
	        	}
	            // SP table
	            if (result.has("trailingPE")) {
	            	quoteRow.put("dPE", result.get("trailingPE").asDouble());
	           }
	        }
    	}
		return quoteRow;
	}
		
	private Map<String, Object> getNextCsvQuote() throws IOException {
		
		Map<String, Object> quoteRow = new HashMap<>();
				
		while (true) {
			// Get next row from CSV file
			String csvRow = csvBr.readLine();
			if (csvRow == null) {
				// End of file
				csvBr.close();
				return null;
			}
			String[] csvColumn = csvRow.split(",");
					
			// Get quote date
			LocalDate quoteLd = LocalDate.parse(csvColumn[0]);
			Date quoteDate = Date.from(quoteLd.atStartOfDay(sysZoneId).toInstant());		
			
			// SEC table columns
			quoteRow.put("szSymbol", symbol);
			// Assume dtLastUpdate is date of quote data in SEC row
			quoteRow.put("dtLastUpdate", quoteDate);
	
			// SP table columns
			quoteRow.put("dt", quoteDate);
			try {
				quoteRow.put("dOpen", Double.parseDouble(csvColumn[1]) * quoteFactor);
			    quoteRow.put("dHigh", Double.parseDouble(csvColumn[2]) * quoteFactor);
			    quoteRow.put("dLow", Double.parseDouble(csvColumn[3]) * quoteFactor);
			    quoteRow.put("dPrice", Double.parseDouble(csvColumn[4]) * quoteFactor);
			    quoteRow.put("vol", Long.parseLong(csvColumn[6]));
			} catch (NumberFormatException e ) {
				// TODO Need to somehow reflect this in system exit code
				LOGGER.warn(e);
				continue;
			}
			return quoteRow;
		}
	}

	/** 
     * Truncate a symbol to the maximum Money symbol length of 12 characters if required.
     * 
     * @param	symbol	the symbol to be truncated
     * @return			the truncated symbol
     */
	private String truncateSymbol(String symbol) {
		String origSymbol = symbol;
	    if (symbol.length() > 12) {
			symbol = symbol.substring(0, 12);
			LOGGER.info("Truncated symbol {} to {}", origSymbol, symbol);
		}
	    return symbol;
    }
	
}