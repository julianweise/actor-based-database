package de.hpi.julianweise.slave.partition.data.comparator.boolencomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = boolean.class, rightSideType = double.class)
@SuppressWarnings("unused")
public class ADBComparatorBoolean2Double extends ADBComparator {

    public ADBComparatorBoolean2Double(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        boolean aBool = this.leftSideField.getBoolean(a);
        if (aBool) {
            return this.rightSideField.getDouble(b) == 0 ? 1 : 0;
        }
        return this.rightSideField.getDouble(b) == 0 ? 0 : -1;
    }
}

