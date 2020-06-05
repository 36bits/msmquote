package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import java.util.Collections;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Column;

public class CliDatTable extends MsmTable {

	// Define CLI_DAT values
	public enum IdData {
		FILENAME(65541, 8, "rgbVal"),
		OLUPDATE(917505, 7, "dtVal");

		private final int code;
		private final int oft;
		private final String valCol;
		
		IdData(int code, int oft, String valCol) {
			this.code = code;
			this.oft = oft;
			this.valCol = valCol;
		}

		public int getCode() {
			return code;
		}
		public int getOft() {
			return oft;
		}
		public String getColumn() {
			return valCol;
		}
	}	

	// Constructor
	public CliDatTable(Database msmDb) throws IOException {
		super(msmDb, "CLI_DAT");		
		return;
	}	

	/** 
	 * Update a value in the CLI_DAT table.
	 * 
	 * @return				
	 */
	public boolean update(int idData, int oft, String column, Object val) throws IOException {
		boolean found = msmCursor.findFirstRow(Collections.singletonMap("idData", idData));
		if (found) {
			Column cliDatCol = msmTable.getColumn("oft");
			msmCursor.setCurrentRowValue(cliDatCol, oft);
			cliDatCol = msmTable.getColumn(column);
			msmCursor.setCurrentRowValue(cliDatCol, val);			
			return true;
		}
		return false;
	}
}