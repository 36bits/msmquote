package uk.co.pueblo.msm.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parent class for all quote sources.
 */
public abstract class QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(QuoteSource.class);
	private static final Pattern PATTERN_MINOR_CURRENCY = Pattern.compile("..([a-z]|X)");
	static final Pattern PATTERN_NUM = Pattern.compile("-?\\d+(\\.\\d+|\\.\\d+E\\d+)?");
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
	public abstract Map<String, Object> getNext() throws IOException;

	/**
	 * Gets the status of the quote source instance.
	 * 
	 * @return source status
	 */
	SourceStatus getStatus() {
		return sourceStatus;
	}

	static Properties loadProperties(String propsFile) throws IOException {
		final Properties props = new Properties();
		InputStream propsIs = QuoteSource.class.getClassLoader().getResourceAsStream(propsFile);
		props.load(propsIs);
		return props;
	}

	static int getAdjuster(Properties props, String currency, String quoteType) {
		int adjuster = 1;
		String prop;
		Matcher matchMinorCurrency = PATTERN_MINOR_CURRENCY.matcher(currency);
		if ((prop = props.getProperty("adjust." + currency + "." + quoteType)) != null) {
			adjuster = Integer.parseInt(prop);
		} else if ((prop = props.getProperty("adjust." + currency)) != null) {
			adjuster = Integer.parseInt(prop);
		} else if (matchMinorCurrency.matches()) { // minor currency units: cents, pence, etc.
			adjuster = 100;
		}
		return adjuster;
	}

	static double adjustQuote(double value, String operation, int adjuster) {
		switch (operation.charAt(0)) {
		case ('d'):
			return value / adjuster;
		case ('m'):
			return value * 100 * adjuster;
		}
		return value;
	}
}