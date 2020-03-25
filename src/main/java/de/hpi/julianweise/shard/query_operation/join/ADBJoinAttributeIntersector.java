package de.hpi.julianweise.shard.query_operation.join;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class ADBJoinAttributeIntersector  {

    public static List<Pair<Integer, Integer>> intersect(List<Pair<Integer, Integer>> setA,
                                                   List<Pair<Integer, Integer>> setB) {
        List<Pair<Integer, Integer>> resultSet = new ArrayList<>(setA.size() + setB.size());
        setA.sort(ADBJoinAttributeIntersector::comparingJoinCandidates);
        setB.sort(ADBJoinAttributeIntersector::comparingJoinCandidates);

        for (int a = 0, b = 0; a < setA.size() && b < setB.size(); b++) {
            Pair<Integer, Integer> lastElement = resultSet.size() > 0 ? resultSet.get(resultSet.size() - 1) : null;
            if (setA.get(a).equals(setB.get(b))) {
                if (lastElement == null || !lastElement.equals(setA.get(a))) resultSet.add(setA.get(a));
                a++;
                continue;
            }
            if (ADBJoinAttributeIntersector.comparingJoinCandidates(setA.get(a), setB.get(b)) < 0) {
                a++;
                b--;
            }
        }
        return resultSet;
    }

    public static int comparingJoinCandidates(Pair<Integer, Integer> a, Pair<Integer, Integer> b) {
        if (a.getKey() - b.getKey() == 0) {
            return a.getValue() - b.getValue();
        }
        return a.getKey() - b.getKey();
    }

}
