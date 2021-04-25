package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

import uk.co.pueblo.msmquote.MsmCore.CliDatRow;
import uk.co.pueblo.msmquote.MsmCore.DhdColumn;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);
	private static final int EXIT_OK = 0;
	private static final int EXIT_WARN = 1;
	private static final int EXIT_ERROR = 2;

	public static void main(String[] args) {

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

		Instant startTime = Instant.now();
		int exitCode = EXIT_OK;
		MsmDb db = null;

		try {	    	
			// Process command-line arguments
			String password = null;
			String sourceArg = null;

			if (args.length == 3) {
				password = args[1];
				sourceArg = args[2];
			} else if (args.length == 2) {
				sourceArg = args[1];
			} else {
				throw new IllegalArgumentException("Usage: filename [password] source");
			}

			// Open Money database
			Database openedDb = null;			
			db = new MsmDb(args[0], password);
			openedDb = db.getDb();

			try {
				// Instantiate table objects needed to process source quote type
				MsmCore msmCore = new MsmCore(openedDb);
				MsmSecurity msmSecurity = new MsmSecurity(openedDb);
				MsmCurrency msmCurrency = new MsmCurrency(openedDb);
				
				// Process quote source types
				YahooQuote yahooQuote = null;

				if (sourceArg.contains("finance.yahoo.com/v7/finance/quote")) {
					if (sourceArg.endsWith("symbols=")  || sourceArg.endsWith("symbols=?")) {
						yahooQuote = new YahooApiQuote(sourceArg, msmSecurity.getSymbols(msmCore), msmCurrency.getIsoCodes(msmCore.getDhdVal(DhdColumn.BASE_CURRENCY.getName())));
					} else {
						yahooQuote = new YahooApiQuote(sourceArg);
					}
				} else if (sourceArg.contains("finance.yahoo.com/v7/finance/chart")) {
					yahooQuote = new YahooApiHist(sourceArg);						
				} else if (sourceArg.endsWith(".csv")) {
					yahooQuote = new YahooCsvHist(sourceArg);
				} else {
					throw new IllegalArgumentException("Unrecogonised quote source");
				}

				if (!yahooQuote.isQuery()) {
					// Update quote data in Money database
					String currencyPair;
					String[] isoCodes = {"", ""};
					int[] hcrncs = {0, 0};
					Map<String, Object> quoteRow = new HashMap<>();				
					while (true) {
						if ((quoteRow = yahooQuote.getNext()) == null) {
							break;
						}
						if (quoteRow.containsKey("xError")) {
							exitCode = EXIT_WARN;
						}
						if (quoteRow.containsKey("dPrice")) {
							// Update stock quote data
							if(!msmSecurity.update(quoteRow)) {
								exitCode = EXIT_WARN;
							}
						} else if (quoteRow.containsKey("dRate")) {
							// Get hcrncs of currency pair
							currencyPair = (String) quoteRow.get("xSymbol");
							isoCodes[0] = currencyPair.substring(0, 3);
							isoCodes[1] = currencyPair.substring(3, 6);
							hcrncs = msmCurrency.getHcrncs(isoCodes);
							// Update exchange rate table
							if (!msmCurrency.update(hcrncs, (double) quoteRow.get("dRate"))) {
								exitCode = EXIT_WARN;
							}
						} else {
							exitCode = EXIT_WARN;
						}
					} 

					// Add any new rows to the SP table
					msmSecurity.addNewSpRows();

					// Update online update time-stamp
					msmCore.updateCliDatVal(CliDatRow.OLUPDATE, LocalDateTime.now());
				}

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occured!", e);
				exitCode = EXIT_ERROR;
			}

			// Close Money database
			db.closeDb();

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occured!", e);
			exitCode = EXIT_ERROR;
		}									
		LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
		System.exit(exitCode);
	}
}