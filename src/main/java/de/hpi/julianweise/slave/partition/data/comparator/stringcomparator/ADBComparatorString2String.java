package de.hpi.julianweise.slave.partition.data.comparator.stringcomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = String.class, rightSideType = String.class)
@SuppressWarnings("unused")
public class ADBComparatorString2String extends ADBComparator {

    public ADBComparatorString2String(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @Override
    @SneakyThrows
    public int compare(Object a, Object b) {
        return ((String) this.leftSideField.get(a)).compareTo((String) this.rightSideField.get(b));
    }
}
