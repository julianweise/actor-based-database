package de.hpi.julianweise.utility.largemessage;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ADBKeyPair {

    private int key;
    private int value;

    public ADBKeyPair(int a, int b) {
        this.key = a;
        this.value = b;
    }
}
