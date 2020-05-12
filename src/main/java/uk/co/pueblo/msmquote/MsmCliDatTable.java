package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;

public class MsmCliDatTable {
	//private static final Logger LOGGER = LogManager.getLogger(MsmCliDatTable.class);

	private Table cliDatTable;
	private IndexCursor cliDatCursor;
	
	// enums for CLI_DAT values
	enum IdData {
		FILENAME(65541, 8, "rgbVal"),
		OLUPDATE(917505, 7, "dtVal");

		private final int code;
		private final int oft;
		private final String column;
		
		IdData(int code, int oft, String valCol) {
			this.code = code;
			this.oft = oft;
			this.column = valCol;
		}

		public int getCode() {
			return code;
		}
		public int getOft() {
			return oft;
		}
		public String getColumn() {
			return column;
		}
	}	

	// Constructor
	public MsmCliDatTable(Database mnyDb) throws IOException {
		cliDatTable = mnyDb.getTable("CLI_DAT");
		cliDatCursor = CursorBuilder.createCursor(cliDatTable.getPrimaryKeyIndex());
		return;
	}	

	/** 
	 * Update a value in the CLI_DAT table.
	 * 
	 * @return				
	 */
	public boolean update(int idData, int oft, String column, Object val) throws IOException {
		Map<String, Object> rowPattern = new HashMap<>();
		rowPattern.put("idData", idData);
		if (cliDatCursor.findFirstRow(rowPattern)) {
			Column cliDatCol = cliDatTable.getColumn("oft");
			cliDatCursor.setCurrentRowValue(cliDatCol, oft);
			cliDatCol = cliDatTable.getColumn(column);
			cliDatCursor.setCurrentRowValue(cliDatCol, val);			
			return true;
		}
		return false;
	}
}