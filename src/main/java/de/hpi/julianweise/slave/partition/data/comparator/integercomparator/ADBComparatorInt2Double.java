package de.hpi.julianweise.slave.partition.data.comparator.integercomparator;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.data.comparator.ADBTypeComparator;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@ADBTypeComparator(leftSideType = int.class, rightSideType = double.class)
@SuppressWarnings("unused")
public class ADBComparatorInt2Double extends ADBComparator {

    public ADBComparatorInt2Double(Field leftSideField, Field rightSideField) {
        super(leftSideField, rightSideField);
    }

    @Override
    @SneakyThrows
    public int compare(Object a, Object b) {
        return Double.compare(this.leftSideField.getInt(a), this.rightSideField.getDouble(b));
    }
}
