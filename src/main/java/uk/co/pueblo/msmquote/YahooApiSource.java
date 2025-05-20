package uk.co.pueblo.msmquote;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.StringJoiner;

import javax.net.ssl.SSLParameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Parent class for the Yahoo Finance API quote sources.
 */
abstract class YahooApiSource extends YahooSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiSource.class);
	private static final String HTTP_REQ_UA = PROPS.getProperty("httprequest.useragent");	
	private static final String[] TLS_VERSIONS = { "TLSv1.3" };
	private static final String CACHE_FILE = "msmquote-cache";
	private static final String COOKIE_NAME = "A3";

	// Class variables
	private static HttpClient httpClient;
	private static CookieManager cookieMgr = new CookieManager();
	private static String crumb = null;

	static {
		// Get Yahoo cookie and API crumb from cache file
		try (BufferedReader br = new BufferedReader(new FileReader(CACHE_FILE))) {
			List<HttpCookie> cookies = HttpCookie.parse(br.readLine()); // get set-cookie
			HttpCookie cookie = cookies.get(0); // assume list has only one cookie
			cookieMgr.getCookieStore().add(null, cookie);
			crumb = br.readLine(); // get crumb
			LOGGER.info("Using cached Yahoo cookie and API crumb");
		} catch (Exception e) {
			sourceClassStatus = SourceStatus.WARN;
			LOGGER.warn(e.getMessage());
		}

		// Set up http client
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("httpclient.timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		SSLParameters sslParams = new SSLParameters();
		sslParams.setProtocols(TLS_VERSIONS);
		httpClient = HttpClient.newBuilder().cookieHandler(cookieMgr).connectTimeout(Duration.ofSeconds(httpClientTimeout)).sslParameters(sslParams).build();
	}

	YahooApiSource() {
	}

	static JsonNode getQuoteData(String apiUrl) throws QuoteSourceException {
		// Get data from the API		
		String apiResponse = null;
		String cacheData = null;
		String failMsg = "Failed to get valid quote data using cached cookie and crumb";
		JsonNode jn = null;
		int n = 0;
		LOGGER.debug("URL={}", apiUrl);

		while (true) {
			try {
				apiResponse = httpClient.send(HttpRequest.newBuilder(new URI(apiUrl + "&crumb=" + crumb)).setHeader("User-Agent", HTTP_REQ_UA).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
				if (apiResponse.matches("^\\{\"(quoteResponse|chart)\":\\{\"result\":\\[.*\\Q],\"error\":null}}\\E$")) {
					// Write cookie and crumb to cache
					if (cacheData != null) {
						try (FileWriter fr = new FileWriter(CACHE_FILE)) {
							fr.write(cacheData);
						} catch (Exception e) {
							LOGGER.debug(e);
							LOGGER.warn(e.getMessage());
							sourceClassStatus = SourceStatus.WARN;
						}
					}
					// Return quote data as JSON node
					ObjectMapper mapper = JsonMapper.builder().build();
					jn = mapper.readTree(apiResponse);
					return jn;
				}
			} catch (Exception e) {
				LOGGER.debug(e);
				LOGGER.warn(e.getMessage());
				sourceClassStatus = SourceStatus.WARN;
			}

			LOGGER.warn("{}, API response={}", failMsg, apiResponse);
			
			failMsg = "Failed to get valid quote data";
			String cookieUrl = PROPS.getProperty("cookie.url." + ++n); 
			if (cookieUrl == null) {
				throw new QuoteSourceException("Failed to get any valid quote data!");
			}

			try {
				LOGGER.info("Getting cookie from {}", cookieUrl);
				httpClient.send(HttpRequest.newBuilder(new URI(cookieUrl)).setHeader("User-Agent", HTTP_REQ_UA).GET().build(), HttpResponse.BodyHandlers.ofString());
				List<HttpCookie> cookies = cookieMgr.getCookieStore().getCookies();
				for (HttpCookie cookie : cookies) {
					if (cookie.getName().equals(COOKIE_NAME)) {
						crumb = httpClient.send(HttpRequest.newBuilder(new URI(PROPS.getProperty("crumb.url"))).setHeader("User-Agent", HTTP_REQ_UA).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
						if (crumb.matches("^\\S{11}$")) {
							LOGGER.info("Got API crumb, response={}", crumb);
							// Create cache data
							StringJoiner cacheCookie = new StringJoiner("; ");
							String expires = OffsetDateTime.now(ZoneOffset.UTC).plus(Duration.ofSeconds(cookie.getMaxAge())).format(DateTimeFormatter.RFC_1123_DATE_TIME);
							cacheCookie.add(cookie.toString());
							cacheCookie.add("Expires=" + expires);
							cacheCookie.add("Domain=" + cookie.getDomain());
							cacheCookie.add("Path=" + cookie.getPath());
							// cacheCookie.add("SameSite=None");
							if (cookie.getSecure())
								cacheCookie.add("Secure");
							if (cookie.isHttpOnly())
								cacheCookie.add("HttpOnly");
							cacheData = cacheCookie.toString() + "\n" + crumb;
							break;
						}
						LOGGER.warn("Failed to get valid API crumb, response={}", crumb.trim());
						sourceClassStatus = SourceStatus.WARN;
						crumb = null;
						continue;
					}
					LOGGER.warn("Failed to get cookie");
					sourceClassStatus = SourceStatus.WARN;
				}
			} catch (Exception e) {
				LOGGER.debug(e);
				LOGGER.warn(e.getMessage());
				sourceClassStatus = SourceStatus.WARN;
			}
		}
	}
}