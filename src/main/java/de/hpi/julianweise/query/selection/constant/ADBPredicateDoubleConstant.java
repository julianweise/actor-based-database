package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateDoubleConstant extends ADBPredicateConstant {

    public double value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateDoubleConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateDoubleConstant)) {
            return false;
        }
        return ((ADBPredicateDoubleConstant) o).value == this.value;
    }

    public boolean equals(double o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
