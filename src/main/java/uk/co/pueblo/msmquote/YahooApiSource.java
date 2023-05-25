package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.net.CookieManager;
import java.net.MalformedURLException;
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
	private static String crumb;

	YahooApiSource(String propsFile) throws URISyntaxException, IOException, InterruptedException {
		super(propsFile);

		// Get http client timeout from properties file
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("httpclient.timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);

		// Get Yahoo cookie and crumb
		httpClient = HttpClient.newBuilder().cookieHandler(new CookieManager()).connectTimeout(Duration.ofSeconds(httpClientTimeout)).build();
		HttpRequest request = buildHttpRequest(PROPS.getProperty("cookie.url")); // cookie
		httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		request = buildHttpRequest(PROPS.getProperty("crumb.url")); // crumb
		crumb = httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
		LOGGER.info("API crumb={}", crumb);
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
		String prop;
		String apiUrl;
		HttpRequest request;
		HttpResponse<String> response;
		while (loop) {
			if (param.startsWith("https://")) {
				apiUrl = param;
				loop = false;
			} else {
				if ((prop = PROPS.getProperty("api.url." + ++n)) == null) {
					break;
				} else {
					apiUrl = prop + param;
				}
			}
			
			apiUrl = apiUrl + "&crumb=" + crumb; // add crumb parameter to url

			try {
				request = buildHttpRequest(apiUrl);
				LOGGER.info("Requesting quote data from Yahoo Finance API, url={}", n);
				response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
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

	static HttpRequest buildHttpRequest(String inUrl) throws URISyntaxException, MalformedURLException {
		URL url = new URL(inUrl);
		URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
		LOGGER.debug(uri.toASCIIString());
		return HttpRequest.newBuilder(uri).GET().build();
	}
}