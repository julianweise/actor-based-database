package de.hpi.julianweise.slave.partition.data.comparator.integercomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = int.class, rightSideType = byte.class)
@SuppressWarnings("unused")
public class ADBComparatorInt2Byte extends ADBComparator {

    public ADBComparatorInt2Byte(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @Override
    @SneakyThrows
    public int compare(Object a, Object b) {
        return Integer.compare(this.leftSideField.getInt(a), this.rightSideField.getByte(b));
    }
}
