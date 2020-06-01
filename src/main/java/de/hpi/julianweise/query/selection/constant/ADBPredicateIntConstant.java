package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateIntConstant extends ADBPredicateConstant {

    public int value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateIntConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateIntConstant)) {
            return false;
        }
        return ((ADBPredicateIntConstant) o).value == this.value;
    }

    public boolean equals(int o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
