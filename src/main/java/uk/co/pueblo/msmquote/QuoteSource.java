package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class QuoteSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(QuoteSource.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	static final Properties PROPS = new Properties();

	// Instance variables
	boolean isQuery = false;

	QuoteSource(String propsFile) {
		// Open properties
		if (!propsFile.isEmpty()) {
			try {
				InputStream propsIs = QuoteSource.class.getClassLoader().getResourceAsStream(propsFile);
				PROPS.load(propsIs);
			} catch (IOException e) {
				LOGGER.fatal(e);
			}
		}
	}

	abstract Map<String, Object> getNext() throws IOException;

	/**
	 * Gets the query status of the quote.
	 *
	 * @return true if this is just a query with no update needed
	 */
	boolean isQuery() {
		return isQuery;
	}
}