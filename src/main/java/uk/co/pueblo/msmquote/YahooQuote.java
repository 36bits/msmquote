package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class YahooQuote {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooQuote.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	private static final String BASE_PROPS = "YahooQuote.properties";

	// Class variables
	static Properties baseProps;

	//Instance variables
	boolean isQuery;
	QuoteSummary quoteSummary;

	static {
		try {
			// Set up base properties			
			InputStream propsIs = YahooApiQuote.class.getClassLoader().getResourceAsStream(BASE_PROPS);
			baseProps = new Properties();
			baseProps.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
	}

	public abstract Map<String, Object> getNext() throws IOException;

	/**
	 * Gets the query status of the quote.
	 *
	 * @return		true if this is just a query with no update needed
	 */
	public boolean isQuery() {
		return isQuery;
	}
}