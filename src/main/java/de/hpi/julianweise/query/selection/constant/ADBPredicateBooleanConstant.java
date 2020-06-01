package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateBooleanConstant extends ADBPredicateConstant {

    public boolean value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateBooleanConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateBooleanConstant)) {
            return false;
        }
        return ((ADBPredicateBooleanConstant) o).value == this.value;
    }

    public boolean equals(boolean o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
