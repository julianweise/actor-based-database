package de.hpi.julianweise.slave.partition.data.comparator.boolencomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = boolean.class, rightSideType = boolean.class)
@SuppressWarnings("unused")
public class ADBComparatorBoolean2Boolean extends ADBComparator {

    public ADBComparatorBoolean2Boolean(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @SneakyThrows
    @Override
    public int compare(Object a, Object b) {
        return Boolean.compare(this.leftSideField.getBoolean(a), this.rightSideField.getBoolean(b));
    }
}
