package de.hpi.julianweise.slave.partition.data.comparator.bytecomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = byte.class, rightSideType = boolean.class)
@SuppressWarnings("unused")
public class ADBComparatorByte2Boolean extends ADBComparator {

    public ADBComparatorByte2Boolean(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        boolean bBoolean = this.rightSideField.getBoolean(b);
        if (bBoolean) {
            return this.leftSideField.getByte(a) == 0 ? -1 : 0;
        }
        return this.leftSideField.getByte(a) == 0 ? 0 : +1;
    }
}
