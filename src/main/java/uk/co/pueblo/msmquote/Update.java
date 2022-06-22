package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.healthmarketscience.jackcess.Database;

import uk.co.pueblo.msmquote.MsmDb.CliDatRow;
import uk.co.pueblo.msmquote.MsmDb.DhdColumn;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);
	private static final int EXIT_OK = 0;
	// private static final int EXIT_WARN = 1;
	private static final int EXIT_ERROR = 2;

	public static void main(String[] args) {

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

		int exitCode, finalExitCode = EXIT_OK;
		final Instant startTime = Instant.now();
		final MsmDb msmDb;

		try {
			// Process command-line arguments
			if (args.length < 3) {
				throw new IllegalArgumentException("Usage: filename password source");
			}

			// Open Money database
			msmDb = new MsmDb(args[0], args[1]);
			final Database openedDb = msmDb.getDb();

			try {
				final MsmSecurity msmSecurity = new MsmSecurity(openedDb);
				final MsmCurrency msmCurrency = new MsmCurrency(openedDb);
				final QuoteSource quoteSource;

				// Instantiate quote object according to quote source
				if (args[2].contains("finance.yahoo.com/v7/finance/quote")) {
					if (args[2].endsWith("symbols=") || args[2].endsWith("symbols=?")) {
						quoteSource = new YahooApiQuote(args[2], msmSecurity.getSymbols(msmDb), msmCurrency.getIsoCodes(msmDb.getDhdVal(DhdColumn.BASE_CURRENCY.getName())));
					} else {
						quoteSource = new YahooApiQuote(args[2]);
					}
				} else if (args[2].contains("finance.yahoo.com/v7/finance/chart")) {
					quoteSource = new YahooApiHist(args[2]);
				} else if (args[2].endsWith(".csv")) {
					quoteSource = new YahooCsvHist(args[2]);
				} else if (args.length == 4 && args[3].startsWith("hist ")) {
					quoteSource = new GoogleSheetsHist(args[2], args[3]);
				} else if (args.length == 4) {
					quoteSource = new GoogleSheetsQuote(args[2], args[3]);
				} else {
					throw new IllegalArgumentException("Unrecognised quote source");
				}

				// Update
				boolean didUpdate = false;
				Map<String, String> quoteRow = new HashMap<>();
				while ((quoteRow = quoteSource.getNext()) != null) {
					didUpdate = true;
					if ((quoteRow.get("xType")).toString().equals("CURRENCY")) {
						// Update exchange rate
						if ((exitCode = msmCurrency.update(quoteRow)) > finalExitCode) {
							finalExitCode = exitCode;
						}
						continue;
					}
					// Update all other quote types
					if ((exitCode = msmSecurity.update(quoteRow)) > finalExitCode) {
						finalExitCode = exitCode;
					}
				}

				if (didUpdate) {
					msmSecurity.addNewSpRows();		// add any new rows to the SP table
					msmDb.updateCliDatVal(CliDatRow.OLUPDATE, LocalDateTime.now());		// update online update time-stamp

					// Print summaries
					msmSecurity.logSummary();
					msmCurrency.logSummary();
				}

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occurred!", e);
				finalExitCode = EXIT_ERROR;
			}

			msmDb.closeDb();	// close Money database

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occurred!", e);
			finalExitCode = EXIT_ERROR;
		}
		LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
		System.exit(finalExitCode);
	}
}