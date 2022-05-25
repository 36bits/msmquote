package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
	 */
	static JsonNode getJson(String url) throws InterruptedException, IOException {		
		// Get http timeout from properties file
		String prop;
		int httpTimeout = ((prop = PROPS.getProperty("api.http.timeout")) != null) ? Integer.parseInt(prop) : 20;
				
		// Build and send http request
		String[] urlSplit = url.split("(?<=\\=)", 2);	// Split parameters out of URL		
		HttpClient httpClient = HttpClient.newHttpClient();
		URI uri = URI.create(urlSplit[0] + URLEncoder.encode(urlSplit[1], StandardCharsets.UTF_8.toString()));
		HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(httpTimeout)).GET().build();
		
		LOGGER.info("Requesting quote data from Yahoo Finance API, request timeout = {}s", httpTimeout);
		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		LOGGER.info("Received {} bytes of quote data", response.body().length());
		
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(response.body());
	}
}