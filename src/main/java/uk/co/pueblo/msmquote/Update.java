package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

public class Update {
	private static final Logger LOGGER = LogManager.getLogger(Update.class);
	
	private static final String DELIM = ",";

	public static void main(String[] args) {

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

		Instant startTime = Instant.now();
		int exitCode = ExitCode.OK.getCode();
		MsmDb db = null;

		try {	    	
			// Process command-line arguments
			String password = null;
			String source = null;

			if (args.length == 3) {
				password = args[1];
				source = args[2];
			} else if (args.length == 2) {
				source = args[1];
			} else {
				throw new IllegalArgumentException("Usage: filename [password] source"); 
			}

			// Open Money database
			Database openedDb = null;			
			db = new MsmDb(args[0], password);
			openedDb = db.getDb();

			try {
				// Instantiate table objects
				MsmSecTable secTable = new MsmSecTable(openedDb);
				MsmSpTable spTable = new MsmSpTable(openedDb);
				MsmFxTable fxTable = new MsmFxTable(openedDb);
				MsmCrncTable crncTable = new MsmCrncTable(openedDb);
				MsmDhdTable dhdTable = new MsmDhdTable(openedDb);
				MsmCliDatTable cliDatTable = new MsmCliDatTable(openedDb);
				
				// Process quote source types
				YahooQuote yahooQuote = null;
				String sourceLow = source.toLowerCase();
				if (sourceLow.startsWith("https://") || sourceLow.startsWith("http://")) {
					if (sourceLow.endsWith("symbols=")) {
						int n;
						String delim;
						// Build stock symbols string
						List<String> symbolsList = secTable.getSymbols();
						String stockSymbols = "";
						for ( n = 0; n < symbolsList.size(); n++) {
							delim = DELIM;
							if (n == 0) {
								delim = "";
							}
							stockSymbols = stockSymbols + delim + symbolsList.get(n);
						}
						LOGGER.info("Will get quote data for these stock symbols: {}", stockSymbols);
						// Build currency symbols string
						String defIsoCode = null;
						String fxSymbols = "";
						List<String> isoCodesList = crncTable.getIsoCodes(dhdTable.getDefHcrnc());
						int isoCodesSz = isoCodesList.size();
						for (n = isoCodesSz; n > 0; n--) {
							if ( n == isoCodesSz) {
								defIsoCode = isoCodesList.get(n - 1);
								continue;
							}
							delim = DELIM;
							if (n == isoCodesSz - 1) {
								delim = "";
							}
							fxSymbols = fxSymbols + delim + defIsoCode + isoCodesList.get(n - 1) + "=X";
						}
						LOGGER.info("Will get quote data for these FX symbols: {}", fxSymbols);
						// Append symbols to Yahoo API URL
						delim = DELIM;
						if (stockSymbols.isEmpty()) {
							delim = "";
						}
						source = source + stockSymbols + delim + fxSymbols;
						LOGGER.debug("{}", source);
					}
					yahooQuote = new YahooQuote(source, true);
				} else if (sourceLow.endsWith(".csv")) {
					yahooQuote = new YahooQuote(source, false);
				} else {
					throw new IllegalArgumentException("Unrecogonised quote source");
				}

				// Process quote data
				int hsec;
				String currencyPair;
				String[] isoCodes = {"", ""};
				int[] hcrncs = {0, 0};
				Map<String, Object> quoteRow = new HashMap<>();
				while (true) {
					if ((quoteRow = yahooQuote.getNext()) == null) {
						break;
					}
					if (quoteRow.containsKey("xError")) {
						exitCode = ExitCode.WARNING.getCode();
					}
					if (quoteRow.get("dPrice") != null) {
						// Update stock quote data
						if((hsec = secTable.update(quoteRow)) != -1) {
							spTable.update(quoteRow, hsec);
						} else {
							exitCode = ExitCode.WARNING.getCode();
						}
					} else if (quoteRow.get("rate") != null) {
						// Get hcrncs of currency pair
						currencyPair = (String) quoteRow.get("symbol");
						isoCodes[0] = currencyPair.substring(0, 3);
						isoCodes[1] = currencyPair.substring(3, 6);
						hcrncs = crncTable.getHcrncs(isoCodes);
						// Update exchange rate table
						if (!fxTable.update(hcrncs, (double) quoteRow.get("rate"))) {
							exitCode = ExitCode.WARNING.getCode();
						}
					} else {
						exitCode = ExitCode.WARNING.getCode();
					}
				}

				// Add any new rows to the SP table
				spTable.addNewRows();
				
				// Update online update time-stamp
				cliDatTable.update(IdData.OLUPDATE.getCode(), IdData.OLUPDATE.getOft(), IdData.OLUPDATE.getColumn(), new Date());

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occured!", e);
				exitCode = ExitCode.ERROR.getCode();
			}

			// Close Money database
			db.closeDb();

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occured!", e);
			exitCode = ExitCode.ERROR.getCode();
		}									
		LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
		System.exit(exitCode);
	}
}