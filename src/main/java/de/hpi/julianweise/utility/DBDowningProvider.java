package de.hpi.julianweise.utility;

import akka.actor.Props;
import akka.cluster.DowningProvider;
import scala.Option;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

public class DBDowningProvider extends DowningProvider {

    @Override
    public FiniteDuration downRemovalMargin() {
        return Duration.create(5, "seconds");
    }

    @Override
    public Option<Props> downingActorProps() {
        return null;
    }
}
