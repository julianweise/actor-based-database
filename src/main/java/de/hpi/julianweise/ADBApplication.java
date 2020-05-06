package de.hpi.julianweise;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.serialization.SerializationExtension;
import akka.serialization.jackson.JacksonCborSerializer;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.csv.CSVParsingActorFactory;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.master.ADBMasterFactory;
import de.hpi.julianweise.master.config.MasterConfiguration;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcess;
import de.hpi.julianweise.master.data_loading.ADBLoadAndDistributeDataProcessFactory;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributorFactory;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import de.hpi.julianweise.query.ADBSelectionQueryTermDeserializer;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.config.SlaveConfiguration;
import de.hpi.julianweise.utility.config.ConfigurationBase;
import de.hpi.julianweise.utility.serialization.CborSerializable;

import java.io.NotSerializableException;
import java.util.HashMap;
import java.util.Map;

public class ADBApplication {

    private Behavior<Void> rootBehavior(ConfigurationBase configuration) {
        return Behaviors.setup(context -> {
            ADBApplication.setCorrectDeserializerForADBEntity(context);
            if (configuration.role().equals(ConfigurationBase.OperationRole.MASTER)) {
                MasterConfiguration masterConfiguration = (MasterConfiguration) configuration;
                Behavior<CSVParsingActor.Command> csvParser =
                        CSVParsingActorFactory.createForFile(masterConfiguration.getInputFile().toAbsolutePath().toString());
                Behavior<ADBDataDistributor.Command> distributor = ADBDataDistributorFactory.createDefault();
                Behavior<ADBLoadAndDistributeDataProcess.Command> loadAndDistributeProcess =
                        ADBLoadAndDistributeDataProcessFactory.createDefault(csvParser, distributor);
                context.spawn(ADBMasterFactory.createDefault(loadAndDistributeProcess), "DBMasterSupervisor");
            } else if (configuration.role().equals(ConfigurationBase.OperationRole.SLAVE)) {
                context.spawn(ADBSlave.create(), "DBSlaveSupervisor");
            }
            return Behaviors.empty();
        });
    }

    private static void setCorrectDeserializerForADBEntity(ActorContext<Void> context) throws NotSerializableException {
        // TODO: Discuss better solution to bind custom deserializer
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBEntity.class, ADBEntityFactoryProvider.getInstance().buildDeserializer());
        module.registerSubtypes(ADBEntityFactoryProvider.getInstance().getTargetClass());
        module.addDeserializer(ADBSelectionQueryTerm.class,
                new ADBSelectionQueryTermDeserializer(ADBEntityFactoryProvider.getInstance().getTargetClass()));
        JacksonCborSerializer serializer = (JacksonCborSerializer) SerializationExtension
                .get(context.getSystem()).serializerFor(CborSerializable.class);
        serializer.objectMapper().registerModule(module);
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

    public ADBApplication(ADBEntityFactory entityFactory) {
        ADBEntityFactoryProvider.initialize(entityFactory);
    }

    public void run(String[] args) {
        ConfigurationBase configuration = ADBApplication.parseArgument(args);
        Config config = configWithPort(configuration.getPort());
        ActorSystem.create(rootBehavior(configuration), "ActorDatabaseSystem", config);
    }
}
