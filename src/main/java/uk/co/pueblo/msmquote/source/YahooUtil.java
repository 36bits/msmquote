package uk.co.pueblo.msmquote.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class YahooUtil {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooUtil.class);

	/**
	 * Gets the quote data from the Yahoo API.
	 * 
	 * @param	apiUrl	the URL for the Yahoo Finance API
	 * @return			the quote data
	 * @throws IOException
	 */
	static JsonNode getJson(String apiUrl) throws IOException {
		LOGGER.info("Requesting quote data from Yahoo API");
		// Using try-with-resources to get AutoClose of InputStream
		try (InputStream quoteIs = new URL(apiUrl).openStream();) {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readTree(quoteIs);
		}
	}

	/**
	 * Generates a Yahoo symbol from the Money symbol.
	 * 
	 * @param	symbol			the Money symbol for the security
	 * @param	country			the Money country for the security
	 * @param	props			the YahooQuote properties
	 * @return					the equivalent Yahoo symbol
	 */
	static String getYahooSymbol(String symbol, String country, Properties props) {
		String yahooSymbol = symbol;
		String prop;
		if (symbol.matches("^\\$US:.*")) {
			// Symbol is in Money index format '$US:symbol'
			if ((prop = props.getProperty("index." + symbol.substring(4))) != null) {
				yahooSymbol = prop;
			}
		} else if (symbol.matches("^\\$..:.*")) {
			// Symbol is in Money index format '$xx:symbol'
			yahooSymbol = "^" + symbol.substring(4);							
		} else if (symbol.matches("^\\$.*")) {
			// Symbol is in Money index format '$symbol'
			yahooSymbol = "^" + symbol.substring(1);
		} else if (symbol.matches("^..:.*")) {
			// Symbol is in Money security format 'xx:symbol'
			if ((prop = props.getProperty("exchange." + country)) != null) {
				yahooSymbol = symbol.substring(3) + prop;
			}
		} else if (!symbol.matches("(.*\\..$|.*\\...$)")){
			// Symbol is not already in Yahoo format 'symbol.x' or 'symbol.xx'
			if ((prop = props.getProperty("exchange." + country)) != null) {
				yahooSymbol = symbol + prop; 
			}
		}		
		return yahooSymbol;
	}
}