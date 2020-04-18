package de.hpi.julianweise.utility;

import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;

public class ADBOffsetCalculator {

    @SuppressWarnings("unchecked")
    public static int[] calc(List<ADBPair<Comparable<?>, Integer>> left, List<ADBPair<Comparable<?>, Integer>> right) {
        int[] offset = new int[left.size()];
        int a = 0, b = 0;
        while(a < left.size() && b < right.size()) {
            Comparable<Object> currentLeftValue = (Comparable<Object>) left.get(a).getKey();
            Object currentRightValue = right.get(b).getKey();
            offset[a] = right.size() - 1;
            if (currentLeftValue.compareTo(currentRightValue) == 0) {
                for(int c = b + 1; c < right.size() && currentRightValue.equals(right.get(c).getKey()); c++) {
                    b++;
                }
                offset[a++] = b;
                continue;
            }
            if (currentLeftValue.compareTo(currentRightValue) < 0) {
                offset[a++] = b;
                continue;
            }
            if (currentLeftValue.compareTo(currentRightValue) > 0) {
                b++;
            }
        }
        while (a < left.size()) {
            offset[a++] = right.size() - 1;
        }
        return offset;
    }
}
