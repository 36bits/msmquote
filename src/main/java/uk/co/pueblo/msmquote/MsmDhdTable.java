package uk.co.pueblo.msmquote;

import java.io.IOException;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public class MsmDhdTable {

	// Instance variables
	private Table dhdTable;

	// Define DHD table columns
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

	/**
	 * Constructor.
	 * 
	 * @param	mnyDb
	 * @throws IOException
	 */
	public MsmDhdTable(Database mnyDb) throws IOException {
		dhdTable = mnyDb.getTable("DHD");
		return;
	}    

	/** 
	 * Get the hcrnc of the base currency.
	 * 
	 * @return				the hcrnc
	 */
	public int getValue(String dhdCol) throws IOException {
		Row row = dhdTable.getNextRow();
		return (int) row.get(dhdCol);        
	}
}