package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class YahooSource extends QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooSource.class);
	private static final String PROPS_FILE = "YahooSource.properties";

	static {
		// Open properties
		try {
			InputStream propsIs = YahooSource.class.getClassLoader().getResourceAsStream(PROPS_FILE);
			PROPS.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
	}

	/**
	 * Generates a Yahoo symbol from the Money symbol.
	 * 
	 * @param symbol  the Money symbol for the security
	 * @param country the Money country for the security
	 * @param props   the YahooQuote properties
	 * @return the equivalent Yahoo symbol
	 */
	static String getYahooSymbol(String symbol, String country) {
		String yahooSymbol = symbol;
		String prop;
		boolean exchangeNotFound = false;
		if (symbol.matches("^\\$US:.*")) {
			// Symbol is in Money index format '$US:symbol'
			if ((prop = PROPS.getProperty("index." + symbol.substring(4))) != null) {
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
			if ((prop = PROPS.getProperty("exchange." + country)) == null) {
				exchangeNotFound = true;
			} else {
				yahooSymbol = symbol.substring(3) + prop;
			}
		} else if (!symbol.matches("(.*\\..{1,3}$|^\\^.*)")) {
			// Symbol is not already in Yahoo format 'symbol.x', 'symbol.xx', 'symbol.xxx' or '^symbol"
			if ((prop = PROPS.getProperty("exchange." + country)) == null) {
				exchangeNotFound = true;
			} else {
				yahooSymbol = symbol + prop;
			}
		}

		if (exchangeNotFound) {
			LOGGER.warn("Cannot find Yahoo Finance exchange suffix for symbol {}, country code={}", symbol, country);
			QuoteSource.setStatus(SOURCE_WARN);
		}

		return yahooSymbol.toUpperCase();
	}
	
	static int getAdjuster(String currency) {
		if (currency.matches("..[a-z]")) { // currency is in cents, pence, etc.
			return 100;
		}
		return 1;
	}

	static String adjustQuote(String value, String operation, int adjuster) {
		if (operation.equals("d")) {
			value = String.valueOf(Double.parseDouble(value) / adjuster);
		} else if (operation.equals("m")) {
			value = String.valueOf(Double.parseDouble(value) * 100 * adjuster);
		}
		return value;
	}
}