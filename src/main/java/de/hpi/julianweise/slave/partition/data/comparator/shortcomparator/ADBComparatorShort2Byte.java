package de.hpi.julianweise.slave.partition.data.comparator.shortcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = short.class, rightSideType = byte.class)
@SuppressWarnings("unused")
public class ADBComparatorShort2Byte extends ADBComparator {

    public ADBComparatorShort2Byte(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        return Short.compare(leftSideField.getShort(a), rightSideField.getByte(b));
    }
}
