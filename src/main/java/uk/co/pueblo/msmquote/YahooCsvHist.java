package uk.co.pueblo.msmquote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
	private int quoteMultiplier;

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

		// Get divisor or multiplier for quote currency and quote type
		String prop;
		quoteDivisor = ((prop = PROPS.getProperty("divisor." + quoteMeta[1] + "." + quoteMeta[2])) == null) ? 1 : Integer.parseInt(prop);
		quoteMultiplier = ((prop = PROPS.getProperty("multiplier." + quoteMeta[1] + "." + quoteMeta[2])) == null) ? 100 : Integer.parseInt(prop);		
	}

	/**
	 * Gets the next row of quote data from the CSV file.
	 * 
	 * @return the quote row or null if no more data
	 * @throws IOException
	 */
	public Map<String, String> getNext() throws IOException {

		Map<String, String> returnRow = new HashMap<>();

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

		// Add quote values to return row
		try {
			String prop;
			for (int n = 0; n < csvColumn.length; n++) {
				if ((prop = PROPS.getProperty("hist.csv." + (n + 1))) != null) {
					String[] csvMap = prop.split(",");
					String value = csvColumn[n];
					if (csvMap.length == 2) {
						if (csvMap[1].equals("d")) {
							value = String.valueOf(Double.parseDouble(value) / quoteDivisor);
						} else if (csvMap[1].equals("m")) {
							value = String.valueOf(Double.parseDouble(value) * quoteMultiplier);
						}
					}
					returnRow.put(csvMap[0], value);
				}
			}
		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}