package exceptions;

public class InternalException extends Exception {
	public static String BASE_ERROR_STRING = "ERROR(200): ";
	public static String INVALID_DATAYPE_EXCEPTION = BASE_ERROR_STRING + "Invalid datatype given.";
	public static String DATAYPE_MISMATCH_EXCEPTION = BASE_ERROR_STRING + "Invalid datatype in WHERE clause. Expected %1.";
	public static String INVALID_CONDITION_EXCEPTION = BASE_ERROR_STRING + "Invalid condition given. Only %1 supported.";
	public static String GENERIC_EXCEPTION = BASE_ERROR_STRING + "An error was encountered performing the operation.";
	
	public InternalException(String message, String parameter)
	{
		super(message.replace("%1", parameter));
	}
	
	public InternalException(String message)
	
	{
		super(message);
	}

}
