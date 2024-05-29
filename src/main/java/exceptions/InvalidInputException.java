package exceptions;

public class InvalidInputException extends DBAppException {

    public InvalidInputException(String strMessage) {
        super(strMessage);
    }
}
