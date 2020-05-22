package uk.co.pueblo.msmquote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MsmUtils {
	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmUtils.class);

	/** 
	 * Truncate a symbol to the maximum Money symbol length of 12 characters.
	 * 
	 * @param	symbol	the symbol to be truncated
	 * @return			the truncated symbol
	 */
	public static String truncateSymbol(String symbol) {
		String origSymbol = symbol;
		if (symbol.length() > 12) {
			symbol = symbol.substring(0, 12);
			LOGGER.info("Truncated symbol {} to {}", origSymbol, symbol);
		}
		return symbol;
	}
}