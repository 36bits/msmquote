package uk.co.pueblo.msmquote;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;

public class MsmDb extends DatabaseBuilder {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmDb.class);
	private static final String DHD_TABLE = "DHD";
	private static final String CLI_DAT_TABLE = "CLI_DAT";
	private static final String CNTRY_TABLE = "CNTRY";

	// Instance variables
	private final Database db;
	private Table dhdTable;
	private Table cliDatTable;
	private Table cntryTable;

	// DHD table columns
	enum DhdColumn {
		BASE_CURRENCY("hcrncDef");

		private final String column;

		DhdColumn(String column) {
			this.column = column;
		}

		public String getName() {
			return column;
		}
	}

	// CLI_DAT table rows
	enum CliDatRow {
		FILENAME(65541, 8, "rgbVal"), OLUPDATE(917505, 7, "dtVal");

		private final int idData;
		private final int oft;
		private final String valCol;

		CliDatRow(int idData, int oft, String valCol) {
			this.idData = idData;
			this.oft = oft;
			this.valCol = valCol;
		}

		public int getIdData() {
			return idData;
		}

		public int getOft() {
			return oft;
		}

		public String getValCol() {
			return valCol;
		}
	}

	// Constructor
	MsmDb(String fileName, String password) throws IOException {

		// Create lock file
		final String lockFileName;
		final int i = fileName.lastIndexOf('.');
		if (i <= 0) {
			lockFileName = fileName;
		} else {
			lockFileName = fileName.substring(0, i);
		}
		final File lockFile = new File(lockFileName + ".lrd");
		LOGGER.info("Creating lock file: {}", lockFile.getAbsolutePath());
		if (!lockFile.createNewFile()) {
			throw new FileAlreadyExistsException("Lock file already exists");
		}
		lockFile.deleteOnExit();

		// Open Money database
		final File dbFile = new File(fileName);
		final CryptCodecProvider cryptCp;

		if (password.length() > 0) {
			cryptCp = new CryptCodecProvider(password);
		} else {
			cryptCp = new CryptCodecProvider();
		}
		LOGGER.info("Opening Money file: {}", dbFile.getAbsolutePath());
		db = new DatabaseBuilder(dbFile).setCodecProvider(cryptCp).open();
		db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

		// Open the core tables
		dhdTable = db.getTable(DHD_TABLE);
		cliDatTable = db.getTable(CLI_DAT_TABLE);
		cntryTable = db.getTable(CNTRY_TABLE);

		return;
	}

	Database getDb() {
		return db;
	}

	void closeDb() throws IOException {
		LOGGER.info("Closing Money file: {}", db.getFile());
		db.close();
		return;
	}

	/**
	 * Get the value of the given column in the DHD table.
	 * 
	 * @param dhdCol the name of the column
	 * @return the hcrnc
	 * @throws IOException
	 */
	int getDhdVal(String dhdCol) throws IOException {
		Row row = dhdTable.getNextRow();
		return (int) row.get(dhdCol);
	}

	/**
	 * Update a value in the CLI_DAT table.
	 * 
	 * @param name   the name of the row to be updated
	 * @param newVal the new value
	 * @return true if successful, otherwise false
	 * @throws IOException
	 */
	boolean updateCliDatVal(CliDatRow name, Object newVal) throws IOException {
		IndexCursor cursor = CursorBuilder.createCursor(cliDatTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("idData", name.getIdData()));
		if (found) {
			Column cliDatCol = cliDatTable.getColumn("oft");
			cursor.setCurrentRowValue(cliDatCol, name.getOft());
			cliDatCol = cliDatTable.getColumn(name.getValCol());
			cursor.setCurrentRowValue(cliDatCol, newVal);
			return true;
		}
		return false;
	}

	/**
	 * Get the country code for the given hcntry.
	 * 
	 * @param hcntry the hcntry to find the country code for
	 * @return the country code or null if not found
	 * @throws IOException
	 */
	String getCntryCode(int hcntry) throws IOException {
		IndexCursor cursor = CursorBuilder.createCursor(cntryTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("hcntry", hcntry));
		if (found) {
			return (String) cursor.getCurrentRowValue(cntryTable.getColumn("szCode"));
		}
		return null;
	}

}