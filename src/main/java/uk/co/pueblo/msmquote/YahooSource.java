package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract class YahooSource implements QuoteSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooSource.class);
	static final int CONNECT_TIMEOUT = 10000;	// milliseconds
	static final int READ_TIMEOUT = 60000;		// milliseconds

	YahooSource(String propsFile) {
		// Open properties
		if (!propsFile.isEmpty()) {
			try {
				InputStream propsIs = getClass().getClassLoader().getResourceAsStream(propsFile);
				PROPS.load(propsIs);
			} catch (IOException e) {
				LOGGER.fatal(e);
			}
		}
	}

	/**
	 * Gets JSON quote data from a web API.
	 * 
	 * @param apiUrl the URL of the web API
	 * @return the quote data
	 * @throws IOException
	 */
	static JsonNode getJson(String apiUrl) throws IOException {
		URL url = new URL(apiUrl);
		URLConnection con = url.openConnection();
		con.setConnectTimeout(CONNECT_TIMEOUT);
		con.setReadTimeout(READ_TIMEOUT);		
		LOGGER.info("Requesting quote data from Yahoo Finance API");
		try (InputStream quoteIs = con.getInputStream();) {		// using try-with-resources to get AutoClose of InputStream
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readTree(quoteIs);
		}
	}
}