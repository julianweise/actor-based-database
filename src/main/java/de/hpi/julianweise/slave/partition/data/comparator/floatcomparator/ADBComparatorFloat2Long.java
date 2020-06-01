package de.hpi.julianweise.slave.partition.data.comparator.floatcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = float.class, rightSideType = long.class)
@SuppressWarnings("unused")
public class ADBComparatorFloat2Long extends ADBComparator {

    public ADBComparatorFloat2Long(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows @Override
    public int compare(Object a, Object b) {
        return Float.compare(this.leftSideField.getFloat(a), this.rightSideField.getLong(b));
    }
}
