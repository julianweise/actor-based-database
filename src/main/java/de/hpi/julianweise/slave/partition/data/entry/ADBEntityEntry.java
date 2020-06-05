package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;

@NoArgsConstructor
public abstract class ADBEntityEntry {

    public static boolean matches(ADBEntityEntry a, ADBEntityEntry b, ADBQueryTerm.RelationalOperator operator) {
        ADBComparator comparator = ADBComparator.getFor(a.getValueField(), b.getValueField());
        switch (operator) {
            case EQUALITY: return comparator.compare(a, b) == 0;
            case LESS: return comparator.compare(a, b) < 0;
            case LESS_OR_EQUAL: return comparator.compare(a, b) <= 0;
            case GREATER: return comparator.compare(a, b) > 0;
            case GREATER_OR_EQUAL: return comparator.compare(a, b) >= 0;
            case INEQUALITY: return comparator.compare(a, b) != 0;
            default: return false;
        }
    }

    @Getter
    private int id;

    public ADBEntityEntry(int id, Field field, ADBEntity entity) {
        this.id = id;
    }

    public abstract Field getValueField();

    @Override
    public abstract int hashCode();
}
