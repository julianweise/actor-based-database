package de.hpi.julianweise.csv;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.master.ADBMasterSupervisor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CSVParsingActor extends AbstractBehavior<CSVParsingActor.Command> {

    public interface Command {
        ActorRef<ADBMasterSupervisor.Response> getClient();
    }

    @Builder
    @AllArgsConstructor
    @Getter
    public static class OpenCSVForParsing implements CSVParsingActor.Command {
        private final String filePath;
        private final ActorRef<ADBMasterSupervisor.Response> client;
        private ADBEntityFactory domainFactory;
        private int chunkSize;
    }

    @Builder
    @AllArgsConstructor
    @Getter
    public static class ParseNextCSVChunk implements CSVParsingActor.Command {
        private final ActorRef<ADBMasterSupervisor.Response> client;
    }

    @Builder
    @Getter
    @AllArgsConstructor
    public static class CSVReadyForParsing implements ADBMasterSupervisor.Response {
        private final ActorRef<Command> respondTo;
    }

    @Getter
    @NoArgsConstructor
    public static class DomainDataChunk implements ADBMasterSupervisor.Response {
        private final List<ADBEntityType> chunk = new ArrayList<>();
    }

    @Getter
    @AllArgsConstructor
    public static class CSVFullyParsed implements ADBMasterSupervisor.Response {
    }

    private InputStreamReader inputStreamReader;
    private CSVParser csvParser;
    private ADBEntityFactory domainFactory;
    private int chunkSize;

    public static Behavior<CSVParsingActor.Command> create() {
        return Behaviors.setup(CSVParsingActor::new);
    }

    public CSVParsingActor(ActorContext<CSVParsingActor.Command> context) {
        super(context);
    }

    @Override public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(OpenCSVForParsing.class, this::handleOpenCSVForParsingCommand)
                .onMessage(ParseNextCSVChunk.class, this::handleParseNextCSVChunk)
                .build();
    }

    private Behavior<Command> handleOpenCSVForParsingCommand(OpenCSVForParsing command) {
        FileInputStream csvFileInputStream = this.locateCSVFile(command.getFilePath());

        if (csvFileInputStream == null) {
            command.getClient().tell(new ADBMasterSupervisor.ErrorResponse("Invalid File provided"));
            return this;
        }

        this.inputStreamReader = new InputStreamReader(csvFileInputStream);
        this.domainFactory = command.domainFactory;
        this.csvParser = this.openCSVForParsing(command.getFilePath());
        this.chunkSize = command.getChunkSize();

        if (this.csvParser == null) {
            command.getClient().tell(new ADBMasterSupervisor.ErrorResponse("Unable to parse CSV"));
            return this;
        }
        command.getClient().tell(new CSVReadyForParsing(this.getContext().getSelf()));
        return this;
    }

    private FileInputStream locateCSVFile(String csvPath) {
        File csvFile = new File(csvPath);
        try {
            return new FileInputStream(csvFile);
        } catch (FileNotFoundException e) {
            this.getContext().getLog().error(String.format("Unable to read csv file %s due to error: %s",
                    csvFile.getAbsolutePath(), e.getLocalizedMessage()));
            e.printStackTrace();
            return null;
        }
    }

    private CSVParser openCSVForParsing(String csvFile) {
        try {
            return CSVFormat.EXCEL.withFirstRecordAsHeader().parse(this.inputStreamReader);
        } catch (IOException e) {
            this.getContext().getLog().error(String.format("Unable to parse csv file %s due to error: %s",
                    csvFile, e.getLocalizedMessage()));
            e.printStackTrace();
            return null;
        }
    }

    private Behavior<Command> handleParseNextCSVChunk(ParseNextCSVChunk command) {
        if (this.csvParser == null) {
            this.getContext().getLog().error("Unable to parse without open CSV file.");
            command.getClient().tell(new ADBMasterSupervisor.ErrorResponse("Open CSV before parsing"));
            return this;
        }
        DomainDataChunk chunk = new DomainDataChunk();
        int counter = 0;
        for (CSVRecord record : csvParser) {
            chunk.getChunk().add(this.domainFactory.build(record));
            counter++;
            if (counter >= this.chunkSize) {
                break;
            }
        }
        if (chunk.chunk.size() < 1) {
            return this.sendCSVConcludedNotification(command);
        }
        command.getClient().tell(chunk);
        return this;
    }

    private Behavior<Command> sendCSVConcludedNotification(ParseNextCSVChunk command) {
        command.getClient().tell(new CSVFullyParsed());
        this.inputStreamReader = null;
        this.csvParser = null;
        return this;
    }

}
