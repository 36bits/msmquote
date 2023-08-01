package uk.co.pueblo.msmquote;

import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parent class for the Yahoo Finance quote sources.
 */
abstract class YahooSource extends QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooSource.class);
	static final Properties PROPS = getProps("YahooSource.properties");

	/**
	 * Attempts to generate a Yahoo Finance symbol from a Money symbol and country code.
	 * 
	 * @param symbol the Money symbol for the security
	 * @param country the Money country code for the security
	 * @return the equivalent Yahoo Finance symbol
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
			QuoteSource.setStatus(SourceStatus.WARN);
		}

		return yahooSymbol.toUpperCase();
	}
}