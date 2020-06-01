package de.hpi.julianweise.slave.partition.data.comparator.integercomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = int.class, rightSideType = long.class)
@SuppressWarnings("unused")
public class ADBComparatorInt2Long extends ADBComparator {

    public ADBComparatorInt2Long(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @Override
    @SneakyThrows
    public int compare(Object a, Object b) {
        return Long.compare(this.leftSideField.getInt(a), this.rightSideField.getLong(b));
    }
}
