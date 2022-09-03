package top.byteinfo.iter.execption;

public class BoundaryConditionException extends RuntimeException{
    public BoundaryConditionException() {
        super();
    }

    public BoundaryConditionException(String message) {
        super(message);
    }
}
