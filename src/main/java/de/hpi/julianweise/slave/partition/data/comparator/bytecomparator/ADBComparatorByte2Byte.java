package de.hpi.julianweise.slave.partition.data.comparator.bytecomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = byte.class, rightSideType = byte.class)
@SuppressWarnings("unused")
public class ADBComparatorByte2Byte extends ADBComparator {

    public ADBComparatorByte2Byte(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        return Byte.compare(leftSideField.getByte(a), rightSideField.getByte(b));
    }
}
