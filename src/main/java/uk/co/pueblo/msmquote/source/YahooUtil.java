package uk.co.pueblo.msmquote.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class YahooUtil {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooUtil.class);

	/**
	 * Get quote data from the Yahoo API.
	 * 
	 * @param	apiUrl	the URL for the Yahoo Finance API
	 * @return			the quote data in JSON
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
	 * Generate Yahoo symbol from Money symbol.
	 * 
	 * @param	symbol			the Money symbol for the security
	 * @param	exchangeCode	the Yahoo exchange code for the security
	 * @return					the equivalent Yahoo symbol
	 */
	static String getYahooSymbol(String symbol, String exchangeCode) {
		String yahooSymbol = null;
		if (symbol.matches("(.*\\..$|.*\\...$|^\\^.*)")) {
			// Symbol is in Yahoo format
			yahooSymbol = symbol;
		} else if (symbol.matches("^\\$..:.*")) {
			// Symbol is in Money index format '$xx:symbol'
			yahooSymbol = "^" + symbol.substring(4);							
		} else if (symbol.matches("^\\$.*")) {
			// Symbol is in Money index format '$symbol'
			yahooSymbol = "^" + symbol.substring(1);
		} else if (symbol.matches("^..:.*")) {
			// Symbol is in Money security format 'xx:symbol'
			if (exchangeCode != null) {
				yahooSymbol = symbol.substring(3) + exchangeCode;
			}
		} else {
			// Symbol is in Money security format for default currency 'symbol'
			if (exchangeCode != null) {
				yahooSymbol = symbol + exchangeCode; 
			}
		}		
		return yahooSymbol;
	}
}