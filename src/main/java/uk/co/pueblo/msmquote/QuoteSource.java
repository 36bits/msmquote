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

	// Class variables
	private static SourceStatus finalStatus = SourceStatus.OK;

	// Source status
	enum SourceStatus {
		OK(0), WARN(1), ERROR(2), FATAL(3);

		final int code;

		SourceStatus(int code) {
			this.code = code;
		}
	}

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
		return finalStatus.code;
	}

	static void setStatus(SourceStatus status) {
		if (status.code > finalStatus.code) {
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