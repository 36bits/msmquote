package uk.co.pueblo.msmquote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class YahooCsvHist extends YahooSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooCsvHist.class);
	private static final String PROPS_FILE = "YahooSource.properties";

	// Instance variables
	private BufferedReader csvBr;
	private String[] quoteMeta;
	private int quoteDivisor;

	/**
	 * Constructor for CSV file quote data source.
	 * 
	 * @param fileName the name of the CSV file
	 * @throws IOException
	 */
	YahooCsvHist(String fileName) throws IOException {
		super(PROPS_FILE);
		File csvFile = new File(fileName);
		csvBr = new BufferedReader(new FileReader(csvFile));
		if (!csvBr.readLine().equals("Date,Open,High,Low,Close,Adj Close,Volume")) {
			LOGGER.warn("Yahoo CSV header not found in file {}", csvFile);
			csvBr.close();
		}

		// Get quote metadata from CSV file name
		String tmp = csvFile.getName();
		quoteMeta = tmp.substring(0, tmp.length() - 4).split(" "); // symbol, currency, quote type
		// Set quote divisor according to currency
		quoteDivisor = 1;
		String quoteDivisorProp = PROPS.getProperty("divisor." + quoteMeta[1] + "." + quoteMeta[2]);
		if (quoteDivisorProp != null) {
			quoteDivisor = Integer.parseInt(quoteDivisorProp);
		}
	}

	/**
	 * Gets the next row of quote data from the CSV file.
	 * 
	 * @return the quote row or null if no more data
	 * @throws IOException
	 */
	public Map<String, Object> getNext() throws IOException {

		Map<String, Object> returnRow = new HashMap<>();

		// Get next row from CSV file
		String csvRow = csvBr.readLine();
		if (csvRow == null) {
			// End of file
			csvBr.close();
			return null;
		}
		String[] csvColumn = csvRow.split(",");

		// Process quote metadata
		returnRow.put("xSymbol", quoteMeta[0]);
		returnRow.put("xType", quoteMeta[2]);

		// Build SP table columns
		try {
			String spColumn;
			Double dValue = 0d;
			LocalDateTime dtValue;
			for (int n = 0; n < csvColumn.length; n++) {
				if ((spColumn = PROPS.getProperty("hist.csv." + (n + 1))) != null) {
					if (spColumn.startsWith("dt")) {
						// Process LocalDateTime values
						dtValue = LocalDateTime.parse(csvColumn[n] + "T00:00:00").atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();
						returnRow.put(spColumn, dtValue);
					} else if (spColumn.startsWith("d")) {
						// Process Double values
						dValue = Double.parseDouble(csvColumn[n]);
						// Process adjustments
						if (Boolean.parseBoolean(PROPS.getProperty("divide." + spColumn))) {
							dValue = dValue / quoteDivisor;
						}
						returnRow.put(spColumn, dValue);
					} else {
						// And finally process Long values
						returnRow.put(spColumn, Long.parseLong(csvColumn[n]));
					}
				}
			}
		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}