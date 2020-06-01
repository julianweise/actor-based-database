package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateShortConstant extends ADBPredicateConstant {

    public short value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateShortConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateShortConstant)) {
            return false;
        }
        return ((ADBPredicateShortConstant) o).value == this.value;
    }

    public boolean equals(short o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
