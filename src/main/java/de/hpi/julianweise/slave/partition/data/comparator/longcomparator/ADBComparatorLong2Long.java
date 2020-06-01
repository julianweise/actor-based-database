package de.hpi.julianweise.slave.partition.data.comparator.longcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = long.class, rightSideType = long.class)
@SuppressWarnings("unused")
public class ADBComparatorLong2Long extends ADBComparator {

    public ADBComparatorLong2Long(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        return Long.compare(this.leftSideField.getLong(a), this.rightSideField.getByte(b));
    }
}
