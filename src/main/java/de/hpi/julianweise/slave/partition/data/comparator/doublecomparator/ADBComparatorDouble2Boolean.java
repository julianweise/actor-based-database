package de.hpi.julianweise.slave.partition.data.comparator.doublecomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = double.class, rightSideType = boolean.class)
@SuppressWarnings("unused")
public class ADBComparatorDouble2Boolean extends ADBComparator {

    public ADBComparatorDouble2Boolean(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        boolean bBoolean = this.rightSideField.getBoolean(b);
        if (bBoolean) {
            return this.leftSideField.getDouble(a) == 0 ? -1 : 0;
        }
        return this.leftSideField.getDouble(a) == 0 ? 0 : +1;
    }
}
