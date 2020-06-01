package de.hpi.julianweise.query.selection.constant;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@AllArgsConstructor
@NoArgsConstructor
@SuppressWarnings("unused")
public class ADBPredicateStringConstant extends ADBPredicateConstant {

    public String value;

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBPredicateStringConstant.class.getDeclaredField("value");
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBPredicateStringConstant)) {
            return false;
        }
        return ((ADBPredicateStringConstant) o).value.equals(this.value);
    }

    public boolean equals(String o) {
        return value.equals(o);
    }

    public String toString() {
        return String.valueOf(this.value);
    }
}
