package de.hpi.julianweise.slave.partition.data;

import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.lang.reflect.Field;


public abstract class ADBEntity implements KryoSerializable {

    @Setter
    @Getter
    private int internalID;

    protected ADBEntity() {
    }

    public abstract ADBKey getPrimaryKey();

    public abstract int getSize();

    protected int calculateStringMemoryFootprint(int numberOfChars) {
        return ((int) Math.ceil(24 + 12 + numberOfChars + 2 * Character.BYTES) / 8) * 8;
    }

    @Override
    public int hashCode() {
        return this.getPrimaryKey().hashCode() + 13;
    }

    public final boolean matches(ADBSelectionQuery query) {
        for (ADBSelectionQueryPredicate predicate : query.getPredicates()) {
            if (!this.matches(predicate)) {
                return false;
            }
        }
        return true;
    }

    @SneakyThrows
    public final boolean matches(ADBSelectionQueryPredicate predicate) {
        Field field = this.getClass().getDeclaredField(predicate.getFieldName());
        ADBComparator comparator = ADBComparator.getFor(field, predicate.getValue().getValueField());
        switch (predicate.getOperator()) {
            case EQUALITY:
                return comparator.compare(this, predicate.getValue()) == 0;
            case INEQUALITY:
                return comparator.compare(this, predicate.getValue()) != 0;
            case LESS:
                return comparator.compare(this, predicate.getValue()) < 0;
            case LESS_OR_EQUAL:
                return comparator.compare(this, predicate.getValue()) <= 0;
            case GREATER:
                return comparator.compare(this, predicate.getValue()) > 0;
            case GREATER_OR_EQUAL:
                return comparator.compare(this, predicate.getValue()) >= 0;
            default:
                return false;
        }
    }
}