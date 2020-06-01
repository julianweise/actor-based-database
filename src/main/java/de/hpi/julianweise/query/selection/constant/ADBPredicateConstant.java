package de.hpi.julianweise.query.selection.constant;

import java.lang.reflect.Field;

public abstract class ADBPredicateConstant {
    public abstract Field getValueField();
    public abstract String toString();
}
