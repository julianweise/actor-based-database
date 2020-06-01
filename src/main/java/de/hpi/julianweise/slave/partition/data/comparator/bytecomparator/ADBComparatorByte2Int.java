package de.hpi.julianweise.slave.partition.data.comparator.bytecomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = byte.class, rightSideType = int.class)
@SuppressWarnings("unused")
public class ADBComparatorByte2Int extends ADBComparator {

    public ADBComparatorByte2Int(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        return Integer.compare(leftSideField.getByte(a), rightSideField.getInt(b));
    }
}
