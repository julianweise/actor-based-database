package de.hpi.julianweise;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.julianweise.master.DBMasterSupervisor;
import de.hpi.julianweise.master.MasterConfiguration;
import de.hpi.julianweise.slave.DBSlaveSupervisor;
import de.hpi.julianweise.slave.SlaveConfiguration;
import de.hpi.julianweise.utility.ConfigurationBase;

import java.util.HashMap;
import java.util.Map;

public class DBQuickStart {
    public static void main(String[] args) {
        ConfigurationBase configuration = DBQuickStart.parseArgument(args);
        Config config = configWithPort(configuration.getPort());
        ActorSystem<Void> system = ActorSystem.create(rootBehavior(configuration), "ActorDatabaseSystem", config);
    }

    private static Behavior<Void> rootBehavior(ConfigurationBase configuration) {
        return Behaviors.setup(context -> {
            if (configuration.role().equals(ConfigurationBase.OperationRole.MASTER)) {
                context.spawn(DBMasterSupervisor.create((MasterConfiguration) configuration), "DBMasterSupervisor");
            } else if (configuration.role().equals(ConfigurationBase.OperationRole.SLAVE)) {
                context.spawn(DBSlaveSupervisor.create(), "DBSlaveSupervisor");
            }
            return Behaviors.empty();
        });
    }

    private static ConfigurationBase parseArgument(String[] args) {
        MasterConfiguration masterCommand = new MasterConfiguration();
        SlaveConfiguration slaveCommand = new SlaveConfiguration();
        JCommander commander = JCommander
                .newBuilder()
                .acceptUnknownOptions(false)
                .addCommand("master", masterCommand)
                .addCommand("slave", slaveCommand)
                .build();

        try {
            commander.parse(args);
            switch (commander.getParsedCommand()) {
                case "master":
                    return masterCommand;
                case "slave":
                    return slaveCommand;
                default:
                    throw new AssertionError();
            }

        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (commander.getParsedCommand() == null) {
                commander.usage();
            } else {
                commander.usage(commander.getParsedCommand());
            }
            System.exit(1);
        }
        return null;
    }

    private static Config configWithPort(int port) {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("akka.remote.artery.canonical.port", port);

        return ConfigFactory.parseMap(overrides).withFallback(ConfigFactory.load());
    }
}
