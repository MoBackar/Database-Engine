package exceptions;

public class ColumnNotFoundException extends DBAppException {
    public ColumnNotFoundException(String message) {
        super(message);
    }
}