package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(QuoteSource.class);
	static final int SOURCE_OK = 0;
	static final int SOURCE_WARN = 1;
	static final int SOURCE_ERROR = 2;
	static final int SOURCE_FATAL = 3;

	// Class variables
	private static int finalStatus = SOURCE_OK;

	abstract Map<String, String> getNext() throws IOException;

	static int getStatus() {
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
		if (operation == null ) {
			// Do nothing
		} else if (operation.equals("d")) {
			value = String.valueOf(Double.parseDouble(value) / adjuster);
		} else if (operation.equals("m")) {
			value = String.valueOf(Double.parseDouble(value) * 100 * adjuster);
		}
		return value;
	}
}