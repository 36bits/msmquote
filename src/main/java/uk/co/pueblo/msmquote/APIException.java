package uk.co.pueblo.msmquote;

public class APIException extends Exception {

	// Constants
	private static final long serialVersionUID = -8286853206651699442L;

	public APIException(String errorMessage) {
		super(errorMessage);
	}
}
