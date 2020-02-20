package de.hpi.julianweise.entity;


import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Tuple<K,V> implements CborSerializable {
    private K key;
    private V value;

    @Override
    public String toString() {
        return String.format("%s : %s", this.key, this.value);
    }
}
