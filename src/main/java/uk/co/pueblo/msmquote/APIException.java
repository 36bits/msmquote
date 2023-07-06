package uk.co.pueblo.msmquote;

class APIException extends Exception {

	// Constants
	private static final long serialVersionUID = -8286853206651699442L;

	APIException(String errorMessage) {
		super(errorMessage);
	}
}
