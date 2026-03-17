package uk.co.pueblo.msm.msmquote;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parent class for the Yahoo Finance quote sources.
 */
abstract class YahooSource extends QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooSource.class);
	private static final Pattern PATTERN_MSM_INDEX = Pattern.compile("\\$..:.+");
	private static final Pattern PATTERN_MSM_SECURITY = Pattern.compile("..:.+");
	private static final Pattern PATTERN_YF_SYMBOL = Pattern.compile("(.+\\..{1,3}$|\\^.+)");
	static final Properties PROPS;

	static {
		Properties props = null;
		try {
			props = loadProperties("YahooSource.properties");
		} catch (Exception e) {
			LOGGER.debug("Exception occured!", e);
			LOGGER.fatal("Failed to load properties: {}", e.getMessage());
		}
		PROPS = props;
	}
		
	/**
	 * Attempts to generate a Yahoo Finance symbol from a Money symbol and country code.
	 * 
	 * @param symbol  the Money symbol for the security
	 * @param country the Money country code for the security
	 * @return the equivalent Yahoo Finance symbol or null if no exchange can be found for the country code
	 */
	static String getYahooSymbol(String symbol, String country) {
		String prop;
		
		// Remove any symbol index suffix
		symbol = symbol.replaceFirst("#\\d+$", "");

		// Index symbols
		if (symbol.charAt(0) == '$') {
			// Symbol is in Money index format
			Matcher matchMsmIndex = PATTERN_MSM_INDEX.matcher(symbol);
			symbol = matchMsmIndex.matches() ? symbol.substring(4) : symbol.substring(1);
			if ((prop = PROPS.getProperty("index." + symbol)) != null) {
				// Symbol has a predefined Yahoo equivalent
				return prop.toUpperCase();
			} else {
				return ("^" + symbol).toUpperCase();
			}
		}

		// Security and currency symbols
		if ((prop = PROPS.getProperty("country." + country)) != null) {
			Matcher matchMsmSecurity = PATTERN_MSM_SECURITY.matcher(symbol);
			Matcher matchYfSymbol = PATTERN_YF_SYMBOL.matcher(symbol);
			if (matchMsmSecurity.matches()) {
				// Symbol is in Money security format 'xx:symbol'
				return (symbol.substring(3) + prop).toUpperCase();
			} else if (!matchYfSymbol.matches()) {
				// Symbol is not already in Yahoo format 'symbol.x', 'symbol.xx', 'symbol.xxx' or '^symbol"
				return (symbol + prop).toUpperCase();
			}
			return symbol.toUpperCase();
		}
		return null;
	}
}