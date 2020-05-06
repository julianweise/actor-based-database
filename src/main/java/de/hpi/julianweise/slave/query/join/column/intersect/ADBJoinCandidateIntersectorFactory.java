package de.hpi.julianweise.slave.query.join.column.intersect;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.query.ADBJoinQuery;
import lombok.SneakyThrows;

import java.net.URLEncoder;

public class ADBJoinCandidateIntersectorFactory {

    public static Behavior<ADBJoinCandidateIntersector.Command> createDefault() {
        return Behaviors.setup(ADBJoinCandidateIntersector::new);
    }

    @SneakyThrows
    public static String getName(ADBJoinQuery query, int partition) {
        return "Intersector-for-partition-" + partition +"-query:" + URLEncoder.encode(query.toString(), "utf-8");
    }

}
