package uk.co.pueblo.msmquote;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;

abstract class GoogleSheetsSource extends QuoteSource {
	
	// Constants
	private static final Logger LOGGER = LogManager.getLogger(GoogleSheetsSource.class);
	static final Properties PROPS;

	// Constants for Google API
	private static final String APPLICATION_NAME = "msmquote Google Sheets source";
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String TOKENS_DIRECTORY_PATH = "tokens";
	private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
	private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
	
	// Instance variables
	final List<List<Object>> quoteRows;
	List<Object> headerRow;
	int quoteIndex = 0;
	
	static {
		Properties props = null;
		try {
			props = loadProperties("GoogleSheetsSource.properties");
		} catch (Exception e) {
			LOGGER.debug("Exception occured!", e);
			LOGGER.fatal("Failed to load properties: {}", e.getMessage());
		}
		PROPS = props;
	}

	// Constructor
	GoogleSheetsSource(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		// Build a new authorised API client service.
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT)).setApplicationName(APPLICATION_NAME).build();
		ValueRange response = service.spreadsheets().values().get(spreadsheetId, range).execute();
		quoteRows = response.getValues();
	}

	/**
	 * Creates an authorised Credential object.
	 * 
	 * @param HTTP_TRANSPORT The network HTTP Transport.
	 * @return An authorised Credential object.
	 * @throws IOException If the credentials.json file cannot be found.
	 */
	private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
		// Load client secrets.
		GoogleClientSecrets clientSecrets;
		InputStream in = GoogleSheetsSource.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
		try (in) {		// Try-with-resources to auto-close InputStream
			if (in == null) {
				throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
			}
			clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
		}
		
		// Build flow and trigger user authorisation request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
				.setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH))).setAccessType("offline").build();
		LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
		return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
	}
}