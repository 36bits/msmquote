package uk.co.pueblo.msmquote;

public class APIErrorException extends Exception {

	// Constants
	private static final long serialVersionUID = -8286853206651699442L;

	public APIErrorException(String errorMessage) {
		super(errorMessage);
	}
}
