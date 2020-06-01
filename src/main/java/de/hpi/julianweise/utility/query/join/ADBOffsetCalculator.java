package de.hpi.julianweise.utility.query.join;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBOffsetCalculator {

    public static int[] calc(ObjectList<ADBEntityEntry> left,
                             ObjectList<ADBEntityEntry> right) {
        int[] offset = new int[left.size()];
        int a = 0, b = 0;
        ADBComparator comparator = null;
        if (left.size() > 0 && right.size() > 0) {
            comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(b).getValueField());
        }
        while(a < left.size() && b < right.size()) {
            offset[a] = right.size() - 1;
            if (comparator.compare(left.get(a), right.get(b)) == 0) {
                for(int c = b + 1; c < right.size() && comparator.compare(right.get(b), right.get(c)) == 0; c++) {
                    b++;
                }
                offset[a++] = b;
                continue;
            }
            if (comparator.compare(left.get(a), right.get(b)) < 0) {
                offset[a++] = b;
                continue;
            }
            if (comparator.compare(left.get(a), right.get(b)) > 0) {
                b++;
            }
        }
        while (a < left.size()) {
            offset[a++] = right.size() - 1;
        }
        return offset;
    }
}
