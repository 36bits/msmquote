package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parent class for all quote sources.
 */
public abstract class QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(QuoteSource.class);
	static final int EXIT_OK = 0;
	static final int EXIT_WARN = 1;
	static final int EXIT_ERROR = 2;

	// Class variables
	static SourceStatus sourceClassStatus = SourceStatus.OK;

	// Instance variables
	SourceStatus sourceStatus = SourceStatus.OK;

	// Source status
	public enum SourceStatus {
		OK(EXIT_OK), WARN(EXIT_WARN), ERROR(EXIT_ERROR);

		public final int exitCode;

		SourceStatus(int exitCode) {
			this.exitCode = exitCode;
		}
	}

	QuoteSource() {
		sourceStatus = sourceClassStatus;
		LOGGER.info(this.getClass().getSimpleName());
	}

	/**
	 * Gets the next row of quote data from the quote source.
	 * 
	 * @return a populated quote row or an empty row if there is no more quote data
	 */
	public abstract Map<String, String> getNext() throws IOException;

	/**
	 * Gets the status of the quote source instance.
	 * 
	 * @return source status
	 */
	SourceStatus getStatus() {
		return sourceStatus;
	}

	static Properties loadProperties(String propsFile) {
		final Properties props = new Properties();
		try {
			InputStream propsIs = QuoteSource.class.getClassLoader().getResourceAsStream(propsFile);
			props.load(propsIs);
		} catch (IOException e) {
			LOGGER.debug("Exception occured!", e);
			LOGGER.fatal("Failed to load properties: {}", e.getMessage());
		}
		return props;
	}

	static int getAdjuster(Properties props, String currency) {
		int adjuster = 1;
		String prop;
		if ((prop = props.getProperty("adjust." + currency)) != null) {
			adjuster = Integer.parseInt(prop);
		} else if (currency.matches("(..[a-z]|..X)")) { // minor currency units: cents, pence, etc.
			adjuster = 100;
		}
		return adjuster;
	}

	static int getAdjuster(Properties props, String currency, String quoteType) {
		int adjuster = 1;
		String prop;
		if ((prop = props.getProperty("adjust." + currency + "." + quoteType)) != null) {
			adjuster = Integer.parseInt(prop);
		} else {
			adjuster = getAdjuster(props, currency);
		}
		return adjuster;
	}

	static String adjustQuote(String value, String operation, int adjuster) {
		if (operation != null) {
			try {
				double adjValue = Double.parseDouble(value);
				switch (operation.charAt(0)) {
				case ('d'):
					adjValue = adjValue / adjuster;
					break;
				case ('m'):
					adjValue = adjValue * 100 * adjuster;
					break;
				}
				DecimalFormat df = new DecimalFormat("0.#");
				df.setMaximumFractionDigits(50);
				value = df.format(adjValue);
			} catch (NumberFormatException e) {
				// Do nothing
			}
		}
		return value;
	}
}