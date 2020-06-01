package de.hpi.julianweise.slave.partition.data.comparator.integercomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = int.class, rightSideType = float.class)
@SuppressWarnings("unused")
public class ADBComparatorInt2Float extends ADBComparator {

    public ADBComparatorInt2Float(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @Override
    @SneakyThrows
    public int compare(Object a, Object b) {
        return Float.compare(this.leftSideField.getInt(a), this.rightSideField.getFloat(b));
    }
}
