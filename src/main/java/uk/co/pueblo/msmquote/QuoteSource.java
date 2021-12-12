package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract class QuoteSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(QuoteSource.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	static final Properties PROPS = new Properties();

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
	 * Gets JSON quote data from a web API.
	 * 
	 * @param apiUrl the URL of the web API
	 * @return the quote data
	 * @throws IOException
	 */
	static JsonNode getJson(String apiUrl) throws IOException {
		LOGGER.info("Requesting quote data from web API");
		try (InputStream quoteIs = new URL(apiUrl).openStream();) {		// using try-with-resources to get AutoClose of InputStream
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readTree(quoteIs);
		}
	}
}