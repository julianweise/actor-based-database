package de.hpi.julianweise.master.query_endpoint;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;

public class ADBQueryEndpointFactory {

    public static Behavior<ADBQueryEndpoint.Command> createDefault(ActorRef<ADBPartitionInquirer.Command> nodeInquirer) {
        return Behaviors.setup(context -> {
            SettingsImpl settings = Settings.SettingsProvider.get(context.getSystem());
            return new ADBQueryEndpoint(context, settings.ENDPOINT_HOSTNAME, settings.ENDPOINT_PORT, nodeInquirer,
                    context.messageAdapter(ADBPartitionInquirer.QueryConclusion.class,
                            ADBQueryEndpoint.NodeInquirerResponseWrapper::new));
        });
    }
}
