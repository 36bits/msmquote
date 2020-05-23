package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class YahooApi {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApi.class);

	/**
	 * Get quote data from the Yahoo API. 
	 * 
	 * @param apiUrl
	 * @return
	 * @throws IOException
	 */
	public static JsonNode getJson(String apiUrl) throws IOException {
		LOGGER.info("Requesting quote data from Yahoo API");
		// Using try-with-resources to get AutoClose of InputStream
		try (InputStream quoteIs = new URL(apiUrl).openStream();) {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readTree(quoteIs);
		}
	}

}
