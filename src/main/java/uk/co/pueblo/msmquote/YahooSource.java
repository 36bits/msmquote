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
			if ((prop = PROPS.getProperty("exchange." + country)) != null) {
				yahooSymbol = symbol.substring(3) + prop;
			}
		} else if (!symbol.matches("(.*\\..$|.*\\...$|^\\^.*)")) {
			// Symbol is not already in Yahoo format 'symbol.x', 'symbol.xx' or '^symbol"
			if ((prop = PROPS.getProperty("exchange." + country)) != null) {
				yahooSymbol = symbol + prop;
			}
		}
		return yahooSymbol.toUpperCase();
	}

	static int getDivisor(String quoteCurrency, String quoteType) {
		String prop;
		return ((prop = PROPS.getProperty("divisor." + quoteCurrency + "." + quoteType)) == null) ? 1 : Integer.parseInt(prop);
	}

	static int getMultiplier(String quoteCurrency, String quoteType) {
		String prop;
		return ((prop = PROPS.getProperty("multiplier." + quoteCurrency + "." + quoteType)) == null) ? 100 : Integer.parseInt(prop);
	}

	static String adjustQuote(String value, String adjuster, int divisor, int multiplier) {
		if (adjuster.equals("d")) {
			value = String.valueOf(Double.parseDouble(value) / divisor);
		} else if (adjuster.equals("m")) {
			value = String.valueOf(Double.parseDouble(value) * multiplier);
		}
		return value;
	}	
}