package uk.co.pueblo.msmquote;

// Exit codes
enum ExitCode {
	OK(0),
	WARNING(1),
	ERROR(2);

	private final int code;

	ExitCode(int code) {
		this.code = code;
	}

	public int getCode() {
		return code;
	}
}

// CLI_DAT values
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
