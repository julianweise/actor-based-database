package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateFloatConstant extends ADBPredicateConstant {

    public float value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateFloatConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateFloatConstant)) {
            return false;
        }
        return ((ADBPredicateFloatConstant) o).value == this.value;
    }

    public boolean equals(float o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
