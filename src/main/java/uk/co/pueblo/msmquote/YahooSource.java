package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract class YahooSource implements QuoteSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooSource.class);

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
	 * @param url the URL of the web API
	 * @return the quote data
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException 
	 */
	static JsonNode getJson(String url) throws InterruptedException, IOException, URISyntaxException {
		// Get http timeout from properties file
		String prop;
		int httpTimeout = ((prop = PROPS.getProperty("api.http.timeout")) != null) ? Integer.parseInt(prop) : 20;

		// Build and send http request
		URL apiUrl = new URL(url);
		URI apiUri = new URI(apiUrl.getProtocol(), apiUrl.getUserInfo(), apiUrl.getHost(), apiUrl.getPort(), apiUrl.getPath(), apiUrl.getQuery(), apiUrl.getRef());
		LOGGER.debug(apiUri.toASCIIString());
		HttpClient httpClient = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder(apiUri).timeout(Duration.ofSeconds(httpTimeout)).GET().build();

		LOGGER.info("Requesting quote data from Yahoo Finance API, request timeout = {}s", httpTimeout);
		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		LOGGER.info("Received {} bytes of quote data", response.body().length());

		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(response.body());
	}

	static int getDivisor(String quoteCurrency, String quoteType) {
		String prop;
		return ((prop = PROPS.getProperty("divisor." + quoteCurrency + "." + quoteType)) == null) ? 1 : Integer.parseInt(prop);
	}

	static int getMultiplier(String quoteCurrency, String quoteType) {
		String prop;
		return ((prop = PROPS.getProperty("multiplier." + quoteCurrency + "." + quoteType)) == null) ? 100 : Integer.parseInt(prop);
	}

	static String adjustQuote(String value, String adjuster, int divisor, int multiplier) {
		if (adjuster.equals("d")) {
			value = String.valueOf(Double.parseDouble(value) / divisor);
		} else if (adjuster.equals("m")) {
			value = String.valueOf(Double.parseDouble(value) * multiplier);
		}
		return value;
	}
}