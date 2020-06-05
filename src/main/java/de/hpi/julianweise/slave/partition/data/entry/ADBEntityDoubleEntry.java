package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = double.class)
public class ADBEntityDoubleEntry extends ADBEntityEntry {

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityDoubleEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    public double value;

    @SneakyThrows
    public ADBEntityDoubleEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        this.value = field.getDouble(entity);
    }

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityDoubleEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Double.hashCode(this.value);
        return result;
    }
}
