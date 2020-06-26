package de.hpi.julianweise.slave.query.join;

import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class ADBPartialJoinResult implements KryoSerializable, Iterable<ADBKeyPair> {

    private final IntArrayList left;
    private final IntArrayList right;

    public ADBPartialJoinResult() {
        this.left = new IntArrayList();
        this.right = new IntArrayList();
    }

    public ADBPartialJoinResult(int capacity) {
        this.left = new IntArrayList(capacity);
        this.right = new IntArrayList(capacity);
    }

    public void addResult(int left, int right) {
        this.left.add(left);
        this.right.add(right);
    }

    public void addAllResults(ADBPartialJoinResult otherResults) {
        this.left.addAll(otherResults.left);
        this.right.addAll(otherResults.right);
    }

    public ADBKeyPair get(int index) {
        return new ADBKeyPair(this.left.getInt(index), this.right.getInt(index));
    }

    public int size() {
        return Math.min(this.left.size(), this.right.size());
    }

    public String toString() {
        int guardedSize = Math.min(this.left.size(), this.right.size());
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < guardedSize; i++) {
            stringBuilder.append(this.left.getInt(i));
            stringBuilder.append(',');
            stringBuilder.append(this.right.getInt(i));
            stringBuilder.append(System.getProperty("line.separator"));
        }
        return stringBuilder.toString();
    }

    @NotNull @Override
    public Iterator<ADBKeyPair> iterator() {
        return new Iterator<ADBKeyPair>() {
            private int pointer = 0;

            @Override
            public boolean hasNext() {
                return this.pointer < Math.min(left.size(), right.size());
            }

            @Override
            public ADBKeyPair next() {
                return new ADBKeyPair(left.getInt(this.pointer), right.getInt(this.pointer++));
            }
        };
    }
}
