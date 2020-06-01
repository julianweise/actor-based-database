package de.hpi.julianweise.slave.partition.data.comparator.floatcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = float.class, rightSideType = short.class)
@SuppressWarnings("unused")
public class ADBComparatorFloat2Short extends ADBComparator {

    public ADBComparatorFloat2Short(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows @Override
    public int compare(Object a, Object b) {
        return Float.compare(this.leftSideField.getFloat(a), this.rightSideField.getShort(b));
    }
}
