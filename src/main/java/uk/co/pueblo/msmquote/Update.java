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

		int exitCode = EXIT_OK;
		final Instant startTime = Instant.now();
		final MsmDb db;

		try {
			// Process command-line arguments
			if (args.length < 3) {
				throw new IllegalArgumentException("Usage: filename password source");
			}

			// Open Money database
			db = new MsmDb(args[0], args[1]);
			final Database openedDb = db.getDb();

			try {
				// Instantiate objects needed to process quote source types
				final MsmCore msmCore = new MsmCore(openedDb);
				final MsmSecurity msmSecurity = new MsmSecurity(openedDb);
				final MsmCurrency msmCurrency = new MsmCurrency(openedDb);

				// Process quote source types
				final Quote quote;

				if (args[2].contains("finance.yahoo.com/v7/finance/quote")) {
					if (args[2].endsWith("symbols=") || args[2].endsWith("symbols=?")) {
						quote = new YahooApiQuote(args[2], msmSecurity.getSymbols(msmCore), msmCurrency.getIsoCodes(msmCore.getDhdVal(DhdColumn.BASE_CURRENCY.getName())));
					} else {
						quote = new YahooApiQuote(args[2]);
					}
				} else if (args[2].contains("finance.yahoo.com/v7/finance/chart")) {
					quote = new YahooApiHist(args[2]);
				} else if (args[2].endsWith(".csv")) {
					quote = new YahooCsvHist(args[2]);
				} else if (args.length == 4) {
					quote = new GoogleSheetsQuote(args[2], args[3]);
				} else {
					throw new IllegalArgumentException("Unrecogonised quote source");
				}

				if (!quote.isQuery()) {
					// Update quote data in Money database
					String currencyPair;
					String[] isoCodes = { "", "" };
					int[] hcrncs = { 0, 0 };
					Map<String, Object> quoteRow = new HashMap<>();
					while (true) {
						if ((quoteRow = quote.getNext()) == null) {
							break;
						}
						if (quoteRow.containsKey("xError")) {
							exitCode = EXIT_WARN;
						}
						if (quoteRow.containsKey("dPrice")) {
							// Update stock quote data
							if (!msmSecurity.update(quoteRow)) {
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