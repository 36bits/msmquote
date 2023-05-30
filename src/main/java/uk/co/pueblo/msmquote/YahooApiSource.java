package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.net.CookieManager;
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

public abstract class YahooApiSource extends YahooSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiSource.class);

	// Class variables
	private static HttpClient httpClient;
	private static String crumb = "";

	static {
		// Set up http client
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("httpclient.timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		CookieManager cm = new CookieManager();
		httpClient = HttpClient.newBuilder().cookieHandler(cm).connectTimeout(Duration.ofSeconds(httpClientTimeout)).build();

		// Get Yahoo cookie and crumb
		int n = 0;
		String cookieUrl;
		try {
			while (true) {
				if ((cookieUrl = PROPS.getProperty("cookie.url." + ++n)) == null) {
					break;
				}
				LOGGER.info("Getting API crumb, cookie url={}", n);
				httpClient.send(HttpRequest.newBuilder(new URI(cookieUrl)).GET().build(), HttpResponse.BodyHandlers.ofString());
				if (!cm.getCookieStore().getCookies().isEmpty()) {
					break;
				}
			}
			crumb = httpClient.send(HttpRequest.newBuilder(new URI(PROPS.getProperty("crumb.url"))).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
		} catch (Exception e) {
			LOGGER.debug("Exception occurred!", e);
			LOGGER.fatal(e);
		}

		if (crumb.isEmpty()) {
			LOGGER.fatal("Could not get API crumb");
		} else {
			LOGGER.info("API crumb={}", crumb);
		}
	}

	/**
	 * Gets JSON quote data from the web API.
	 * 
	 * @param param either the URL of the web API or a comma-separated list of symbols
	 * @return the quote data
	 * @throws APIException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	static JsonNode getJson(String param) throws APIException {

		// Get data from the API
		boolean loop = true;
		int n = 0;
		String apiUrl;
		HttpResponse<String> response;
		while (loop) {
			if (param.startsWith("https://")) {
				apiUrl = param;
				loop = false;
			} else {
				if ((apiUrl = PROPS.getProperty("api.url." + ++n)) == null) {
					break;
				} else {
					apiUrl = apiUrl + param;
				}
			}

			apiUrl = apiUrl + "&crumb=" + crumb; // add crumb parameter to url

			try {
				URL url = new URL(apiUrl);
				URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
				LOGGER.info("Requesting quote data from Yahoo Finance API, url={}", n);
				response = httpClient.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
				LOGGER.info("Received {} bytes from Yahoo Finance API", response.body().length());

				ObjectMapper mapper = new ObjectMapper();
				JsonNode responseJn = mapper.readTree(response.body());
				if (responseJn.at("/finance/error").has("code")) {
					throw new APIException("Yahoo Finance API error response: " + responseJn.at("/finance/error").get("code").asText() + ", " + responseJn.at("/finance/error").get("description").asText());
				}
				return responseJn;
			} catch (Exception e) {
				LOGGER.error(e);
				setStatus(SOURCE_ERROR);
			}
		}
		throw new APIException("All Yahoo Finance API requests failed!");
	}
}