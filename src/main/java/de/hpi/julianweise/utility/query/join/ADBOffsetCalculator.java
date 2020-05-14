package de.hpi.julianweise.utility.query.join;

import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;

public class ADBOffsetCalculator {

    public static int[] calc(List<ADBPair<Comparable<Object>, Integer>> left,
                             List<ADBPair<Comparable<Object>, Integer>> right) {
        int[] offset = new int[left.size()];
        int a = 0, b = 0;
        while(a < left.size() && b < right.size()) {
            offset[a] = right.size() - 1;
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) == 0) {
                for(int c = b + 1; c < right.size() && right.get(b).getKey().equals(right.get(c).getKey()); c++) {
                    b++;
                }
                offset[a++] = b;
                continue;
            }
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) < 0) {
                offset[a++] = b;
                continue;
            }
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) > 0) {
                b++;
            }
        }
        while (a < left.size()) {
            offset[a++] = right.size() - 1;
        }
        return offset;
    }
}