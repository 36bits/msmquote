package uk.co.pueblo.msmquote;

public class APIResponseException extends Exception {

	// Constants
	private static final long serialVersionUID = -8286853206651699442L;

	public APIResponseException(String errorMessage) {
		super(errorMessage);
	}
}
