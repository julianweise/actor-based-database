package de.hpi.julianweise.domain.key;

public class ADBKeyComparisonException extends Exception {

    private Class<? extends ADBKey> expectedClass;
    private Class<? extends ADBKey> actualClass;

    public ADBKeyComparisonException(Class<? extends ADBKey> expectedClass, Class<? extends ADBKey> actualClass) {
        this.expectedClass = expectedClass;
        this.actualClass = actualClass;
    }

    @Override
    public String toString() {
        return String.format("ADBKey comparison failed: Expected %s but got %s", this.expectedClass, this.actualClass);
    }
}
