package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.healthmarketscience.jackcess.Database;

import uk.co.pueblo.msmcore.MsmCurrency;
import uk.co.pueblo.msmcore.MsmDb;
import uk.co.pueblo.msmcore.MsmDb.CliDatRow;
import uk.co.pueblo.msmcore.MsmDb.DhdColumn;
import uk.co.pueblo.msmcore.MsmSecurity;

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
			if (args.length < 2) {
				throw new IllegalArgumentException("Usage: filename password [source]");
			}

			// Open Money database
			msmDb = new MsmDb(args[0], args[1]);
			final Database openedDb = msmDb.getDb();

			Map<String, int[]> summary = new HashMap<>();

			try {
				final MsmSecurity msmSecurity = new MsmSecurity(openedDb);
				final MsmCurrency msmCurrency = new MsmCurrency(openedDb);
				final QuoteSource quoteSource;

				// Instantiate quote object according to quote source
				if (args.length == 2) {
					quoteSource = new YahooApiQuote(msmSecurity.getSymbols(msmDb), msmCurrency.getIsoCodes(msmDb.getDhdVal(DhdColumn.BASE_CURRENCY.getName())));
				} else if (args[2].contains("finance.yahoo.com/v7/finance/quote")) {
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
				Map<String, String> quoteRow = new HashMap<>();
				String quoteType;
				while ((quoteRow = quoteSource.getNext()) != null) {
					quoteType = quoteRow.get("xType").toString();
					if (quoteType.equals("CURRENCY")) {
						exitCode = msmCurrency.update(quoteRow); // update currency FX rates
					} else {
						exitCode = msmSecurity.update(quoteRow); // update other security types
					}
					// Process exit code
					if (exitCode > finalExitCode) {
						finalExitCode = exitCode;
					}
					summary.putIfAbsent(quoteType, new int[4]); // OK, skipped, warnings, errors
					int[] count = summary.get(quoteType);
					count[exitCode]++;
					summary.put(quoteType, count);
				}
				
				if (!summary.isEmpty()) {
					msmSecurity.addNewSpRows(); // add any new rows to the SP table
					msmDb.updateCliDatVal(CliDatRow.OLUPDATE, LocalDateTime.now()); // update online update time-stamp
					// Print update summary
					summary.forEach((key, count) -> {
						int processed = count[0] + count[1] + count[2] + count[3];
						LOGGER.info("Summary for quote type {}: processed={}, OK={}, skipped={}, warnings={}, errors={}", key, processed, count[0], count[1], count[2], count[3]);
					});
				}

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occurred!", e);
				finalExitCode = EXIT_ERROR;
			}

			msmDb.closeDb(); // close Money database

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occurred!", e);
			finalExitCode = EXIT_ERROR;
		}
		LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
		System.exit(finalExitCode);
	}
}