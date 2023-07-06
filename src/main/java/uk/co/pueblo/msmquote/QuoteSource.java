package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
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
	static final int SOURCE_OK = 0;
	static final int SOURCE_WARN = 1;
	static final int SOURCE_ERROR = 2;
	static final int SOURCE_FATAL = 3;

	// Class variables
	private static int finalStatus = SOURCE_OK;

	/**
	 * Gets the next row of quote data from the quote source.
	 * 
	 * @return a populated quote row or an empty row if there is no more quote data
	 */
	public abstract Map<String, String> getNext() throws IOException;

	/**
	 * Gets the highest status code for all quote source instances.
	 * 
	 * @return status code
	 */
	public static int getStatus() {
		return finalStatus;
	}

	static void setStatus(int status) {
		if (status > finalStatus) {
			finalStatus = status;
		}
		return;
	}

	static Properties getProps(String propsFile) {
		final Properties props = new Properties();
		try {
			InputStream propsIs = QuoteSource.class.getClassLoader().getResourceAsStream(propsFile);
			props.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
		return props;
	}

	static int getAdjuster(String currency) {
		if (currency.matches("(..[a-z]|..X|ZAC)")) { // minor currency units: cents, pence, etc.
			return 100;
		}
		return 1;
	}

	static String adjustQuote(String value, String operation, int adjuster) {
		if (operation != null) {
			try {
				switch (operation) {
				case ("d"):
					value = String.valueOf(Double.parseDouble(value) / adjuster);
					break;
				case ("m"):
					value = String.valueOf(Double.parseDouble(value) * 100 * adjuster);
					break;
				}
			} catch (NumberFormatException e) {
				// Do nothing
			}
		}
		return value;
	}
}