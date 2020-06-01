package de.hpi.julianweise.slave.partition.data.comparator.shortcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = short.class, rightSideType = boolean.class)
@SuppressWarnings("unused")
public class ADBComparatorShort2Boolean extends ADBComparator {

    public ADBComparatorShort2Boolean(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        boolean bBoolean = this.rightSideField.getBoolean(b);
        if (bBoolean) {
            return this.leftSideField.getShort(a) == 0 ? -1 : 0;
        }
        return this.leftSideField.getShort(a) == 0 ? 0 : +1;
    }
}
