package uk.co.pueblo.msmquote;

import java.util.Properties;

/**
 * Parent class for the Yahoo Finance quote sources.
 */
abstract class YahooSource extends QuoteSource {

	// Constants
	static final Properties PROPS = getProps("YahooSource.properties");

	/**
	 * Attempts to generate a Yahoo Finance symbol from a Money symbol and country code.
	 * 
	 * @param symbol  the Money symbol for the security
	 * @param country the Money country code for the security
	 * @return the equivalent Yahoo Finance symbol or null if no exchange can be found for the country code
	 */
	static String getYahooSymbol(String symbol, String country) {
		String prop;

		// Index symbols
		if (symbol.charAt(0) == '$') {
			// Symbol is in Money index format
			symbol = symbol.matches("^\\$..:.*") ? symbol.substring(4) : symbol.substring(1);
			if ((prop = PROPS.getProperty("index." + symbol)) != null) {
				// Symbol has a predefined Yahoo equivalent
				return prop.toUpperCase();
			} else {
				return ("^" + symbol).toUpperCase();
			}
		}

		// Security symbols
		if ((prop = PROPS.getProperty("exchange." + country)) != null) {
			if (symbol.matches("^..:.*")) {
				// Symbol is in Money security format 'xx:symbol'
				return (symbol.substring(3) + prop).toUpperCase();
			} else if (!symbol.matches("(.*\\..{1,3}$|^\\^.*)")) {
				// Symbol is not already in Yahoo format 'symbol.x', 'symbol.xx', 'symbol.xxx' or '^symbol"
				return (symbol + prop).toUpperCase();
			}
			return symbol.toUpperCase();
		}
		return null;
	}
}