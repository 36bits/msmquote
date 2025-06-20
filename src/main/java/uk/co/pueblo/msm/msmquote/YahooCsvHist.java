package uk.co.pueblo.msm.msmquote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Yahoo Finance CSV file historical quote source.
 */
public class YahooCsvHist extends YahooSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooCsvHist.class);

	// Instance variables
	private BufferedReader csvBr;
	private String[] quoteMeta;
	private int quoteAdjuster;

	/**
	 * Constructs a Yahoo Finance CSV file historical quote source.
	 * 
	 * @param fileName the name of the CSV file containing the historical quote data
	 */
	public YahooCsvHist(String fileName) throws IOException {
		File csvFile = new File(fileName);
		csvBr = new BufferedReader(new FileReader(csvFile));
		if (!csvBr.readLine().equals("Date,Open,High,Low,Close,Adj Close,Volume")) {
			LOGGER.warn("Yahoo CSV header not found in file {}", csvFile);
			csvBr.close();
		}

		// Get quote metadata from CSV file name
		String tmp = csvFile.getName();
		quoteMeta = tmp.substring(0, tmp.length() - 4).split(" "); // symbol, currency, quote type

		// Get quote adjuster for currency
		quoteAdjuster = getAdjuster(PROPS, quoteMeta[1], quoteMeta[2]);
	}

	public Map<String, String> getNext() throws IOException {

		Map<String, String> returnRow = new HashMap<>();

		// Get next row from CSV file
		String csvRow = csvBr.readLine();
		if (csvRow == null) {
			// End of file
			csvBr.close();
			return returnRow;
		}
		String[] csvColumn = csvRow.split(",");

		// Process quote metadata
		returnRow.put("xSymbol", quoteMeta[0]);
		returnRow.put("xType", quoteMeta[2]);

		// Add quote values to return row
		try {
			String prop;
			for (int n = 0; n < csvColumn.length; n++) {
				if ((prop = PROPS.getProperty("hist.csv." + (n + 1))) != null) {
					String[] columnMap = prop.split(",");
					String value = csvColumn[n];
					value = columnMap.length == 2 ? adjustQuote(value, columnMap[1], quoteAdjuster) : value;
					returnRow.put(columnMap[0], value);
				}
			}
		} catch (NumberFormatException e) {
			LOGGER.warn(e.getMessage());
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}