package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

import uk.co.pueblo.msmquote.msm.CliDatTable;
import uk.co.pueblo.msmquote.msm.CntryTable;
import uk.co.pueblo.msmquote.msm.CrncTable;
import uk.co.pueblo.msmquote.msm.Db;
import uk.co.pueblo.msmquote.msm.DhdTable;
import uk.co.pueblo.msmquote.msm.FxTable;
import uk.co.pueblo.msmquote.msm.SecTable;
import uk.co.pueblo.msmquote.msm.SpTable;
import uk.co.pueblo.msmquote.msm.CliDatTable.IdData;
import uk.co.pueblo.msmquote.msm.DhdTable.DhdColumn;
import uk.co.pueblo.msmquote.source.Quote;
import uk.co.pueblo.msmquote.source.YahooApiHist;
import uk.co.pueblo.msmquote.source.YahooApiQuote;
import uk.co.pueblo.msmquote.source.YahooCsvHist;
import uk.co.pueblo.msmquote.source.YahooQuote;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);

	// Define exit codes
	private enum ExitCode {
		OK(0), WARNING(1), ERROR(2);

		private final int code;

		ExitCode(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}
	}

	public static void main(String[] args) {

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

		Instant startTime = Instant.now();
		int exitCode = ExitCode.OK.getCode();
		Db db = null;

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
			db = new Db(args[0], password);
			openedDb = db.getDb();

			try {
				// Instantiate table objects needed to process source quote type
				SecTable secTable = new SecTable(openedDb);
				CrncTable crncTable = new CrncTable(openedDb);
				DhdTable dhdTable = new DhdTable(openedDb);
				CntryTable cntryTable = new CntryTable(openedDb);

				// Process quote source types
				YahooQuote yahooQuote = null;

				if (sourceArg.contains("finance.yahoo.com/v7/finance/quote")) {
					if (sourceArg.endsWith("symbols=")  || sourceArg.endsWith("symbols=?")) {
						yahooQuote = new YahooApiQuote(sourceArg, secTable.getSymbols(cntryTable), crncTable.getIsoCodes(dhdTable.getValue(DhdColumn.BASE_CURRENCY.getName())));
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
					// Instantiate table objects needed to process quote data
					SpTable spTable = new SpTable(openedDb);
					FxTable fxTable = new FxTable(openedDb);
					CliDatTable cliDatTable = new CliDatTable(openedDb);

					// Now update quote data in Money database
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
						if (quoteRow.containsKey("dPrice")) {
							// Update stock quote data
							if((hsec = secTable.update(quoteRow)) != -1) {
								spTable.update(quoteRow, hsec);
							} else {
								exitCode = ExitCode.WARNING.getCode();
							}
						} else if (quoteRow.containsKey("dRate")) {
							// Get hcrncs of currency pair
							currencyPair = (String) quoteRow.get("xSymbol");
							isoCodes[0] = currencyPair.substring(0, 3);
							isoCodes[1] = currencyPair.substring(3, 6);
							hcrncs = crncTable.getHcrncs(isoCodes);
							// Update exchange rate table
							if (!fxTable.update(hcrncs, (double) quoteRow.get("dRate"))) {
								exitCode = ExitCode.WARNING.getCode();
							}
						} else {
							exitCode = ExitCode.WARNING.getCode();
						}
					} 

					// Add any new rows to the SP table
					spTable.addNewRows();

					// Update online update time-stamp
					cliDatTable.update(IdData.OLUPDATE.getCode(), IdData.OLUPDATE.getOft(), IdData.OLUPDATE.getColumn(), LocalDateTime.now());
				}

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