package de.hpi.julianweise.slave.partition.data.comparator.doublecomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = double.class, rightSideType = long.class)
@SuppressWarnings("unused")
public class ADBComparatorDouble2Long extends ADBComparator {

    public ADBComparatorDouble2Long(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        return Double.compare(this.leftSideField.getDouble(a), this.rightSideField.getLong(b));
    }
}
