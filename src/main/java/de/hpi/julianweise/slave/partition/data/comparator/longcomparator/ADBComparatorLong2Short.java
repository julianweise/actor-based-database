package de.hpi.julianweise.slave.partition.data.comparator.longcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = long.class, rightSideType = short.class)
@SuppressWarnings("unused")
public class ADBComparatorLong2Short extends ADBComparator {

    public ADBComparatorLong2Short(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @Override
    @SneakyThrows
    public int compare(Object a, Object b) {
        return Long.compare(this.leftSideField.getLong(a), this.rightSideField.getShort(b));
    }
}
