package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateByteConstant extends ADBPredicateConstant {

    public byte value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateByteConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ADBPredicateByteConstant)) {
            return false;
        }
        return ((ADBPredicateByteConstant) o).value == this.value;
    }

    public boolean equals(byte o) {
        return o == this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
