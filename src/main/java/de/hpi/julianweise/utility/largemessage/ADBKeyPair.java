package de.hpi.julianweise.utility.largemessage;

public class ADBKeyPair extends ADBPair<Integer, Integer> {

    public ADBKeyPair(int a, int b) {
        super(a, b);
    }

    public ADBKeyPair flip() {
        return new ADBKeyPair(this.getValue(), this.getKey());
    }
}
