package uk.co.pueblo.msmquote;

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
import java.util.prefs.Preferences;

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
	private static final String HTTP_USER_AGENT = PROPS.getProperty("http.user-agent");
	private static final String COOKIE_NAME = PROPS.getProperty("http.cookie-name");
	private static final Preferences PREFS_NODE = Preferences.userNodeForPackage(QuoteSource.class);
	private static final String PREF_KEY_COOKIE = PROPS.getProperty("pref-key.cookie");
	private static final String PREF_KEY_CRUMB = PROPS.getProperty("pref-key.crumb");

	// Class variables
	private static HttpClient httpClient;
	private static CookieManager cookieMgr = new CookieManager();
	private static String crumb = null;

	static {
		// Get cached Yahoo API cookie and crumb from preferences
		try {
			List<HttpCookie> cookies = HttpCookie.parse(PREFS_NODE.get(PREF_KEY_COOKIE, null));
			HttpCookie cookie = cookies.get(0);
			cookieMgr.getCookieStore().add(null, cookie);
			crumb = PREFS_NODE.get(PREF_KEY_CRUMB, null);
			LOGGER.info("Using cached Yahoo cookie and API crumb");
		} catch (Exception e) {
			LOGGER.debug(e);
			LOGGER.warn("Failed to get cached Yahoo cookie and API crumb");
			sourceClassStatus = SourceStatus.WARN;
		}

		// Set up http client
		String[] tlsVersions = PROPS.getProperty("http.tls-versions").split("\\s");
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("http.client-timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		SSLParameters sslParams = new SSLParameters();
		sslParams.setProtocols(tlsVersions);
		httpClient = HttpClient.newBuilder().cookieHandler(cookieMgr).connectTimeout(Duration.ofSeconds(httpClientTimeout)).sslParameters(sslParams).build();
	}

	YahooApiSource() {
	}

	static JsonNode getQuoteData(String apiUrl) throws QuoteSourceException {
		// Get data from the API
		String apiResponse = null;
		StringJoiner cookieSj = new StringJoiner("; ");
		JsonNode jn = null;
		int n = 0;
		LOGGER.debug("URL={}", apiUrl);

		while (true) {
			try {
				apiResponse = httpClient.send(HttpRequest.newBuilder(new URI(apiUrl + "&crumb=" + crumb)).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
				if (apiResponse.matches("^\\{\"(quoteResponse|chart)\":\\{\"result\":\\[.*\\Q],\"error\":null}}\\E$")) {
					// Write cookie and crumb to preferences
					if (cookieSj.length() > 0) {
						PREFS_NODE.put(PREF_KEY_COOKIE, cookieSj.toString());
						PREFS_NODE.put(PREF_KEY_CRUMB, crumb);
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

			LOGGER.warn("Failed to get valid quote data, API response={}", apiResponse);

			String cookieUrl = PROPS.getProperty("url.cookie." + ++n);
			if (cookieUrl == null)
				throw new QuoteSourceException("Failed to get any valid quote data!");

			try {
				LOGGER.info("Getting cookie from cookie URL #{}: {}", n, cookieUrl);
				httpClient.send(HttpRequest.newBuilder(new URI(cookieUrl)).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString());
				List<HttpCookie> cookies = cookieMgr.getCookieStore().getCookies();
				for (HttpCookie cookie : cookies) {
					if (cookie.getName().equals(COOKIE_NAME)) {
						crumb = httpClient.send(HttpRequest.newBuilder(new URI(PROPS.getProperty("url.crumb"))).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
						if (crumb.matches("^\\S{11}$")) {
							LOGGER.info("Got API crumb, response={}", crumb);
							// Create Set-Cookie header
							String expires = OffsetDateTime.now(ZoneOffset.UTC).plus(Duration.ofSeconds(cookie.getMaxAge())).format(DateTimeFormatter.RFC_1123_DATE_TIME);
							cookieSj.add(cookie.toString());
							cookieSj.add("Expires=" + expires);
							cookieSj.add("Domain=" + cookie.getDomain());
							cookieSj.add("Path=" + cookie.getPath());
							// cacheCookie.add("SameSite=None");
							if (cookie.getSecure())
								cookieSj.add("Secure");
							if (cookie.isHttpOnly())
								cookieSj.add("HttpOnly");
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