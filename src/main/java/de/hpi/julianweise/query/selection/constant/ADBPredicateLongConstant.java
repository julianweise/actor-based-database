package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateLongConstant extends ADBPredicateConstant {

    public long value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateLongConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateLongConstant)) {
            return false;
        }
        return ((ADBPredicateLongConstant) o).value == this.value;
    }

    public boolean equals(long o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
