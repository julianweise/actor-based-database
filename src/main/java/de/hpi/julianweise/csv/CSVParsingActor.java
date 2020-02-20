package de.hpi.julianweise.csv;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.entity.Tuple;
import de.hpi.julianweise.master.DBMasterSupervisor;
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

    private static int MAX_ROWS_PER_CHUNK = 100;

    public interface Command {
        ActorRef<DBMasterSupervisor.Response> getClient();
    }

    @Builder
    @AllArgsConstructor
    @Getter
    public static class OpenCSVForParsing implements CSVParsingActor.Command {
        private final String filePath;
        private final ActorRef<DBMasterSupervisor.Response> client;
    }

    @Builder
    @AllArgsConstructor
    @Getter
    public static class ParseNextCSVChunk implements CSVParsingActor.Command {
        private final ActorRef<DBMasterSupervisor.Response> client;
    }

    @Getter
    @NoArgsConstructor
    public static class CSVReadyForParsing implements DBMasterSupervisor.Response {
    }

    @Getter
    @NoArgsConstructor
    public static class CSVChunk implements DBMasterSupervisor.Response {
        private final List<CSVRow> chunk = new ArrayList<>();
    }

    private InputStreamReader inputStreamReader;
    private CSVParser csvParser;

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
            command.getClient().tell(new DBMasterSupervisor.ErrorResponse("Invalid File provided"));
            return this;
        }

        this.inputStreamReader = new InputStreamReader(csvFileInputStream);
        this.csvParser = this.openCSVForParsing(command.getFilePath());

        if (this.csvParser == null) {
            command.getClient().tell(new DBMasterSupervisor.ErrorResponse("Unable to parse CSV"));
            return this;
        }
        command.getClient().tell(new CSVReadyForParsing());
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
            command.getClient().tell(new DBMasterSupervisor.ErrorResponse("Open CSV before parsing"));
            return this;
        }

        List<String> headers = csvParser.getHeaderNames();
        CSVChunk chunk = new CSVChunk();
        int counter = 0;
        for (CSVRecord record : csvParser) {
            CSVRow row = new CSVRow();
            for (String header : headers) {
                row.addTuple(new Tuple<>(header, record.get(header)));
            }
            chunk.chunk.add(row);
            counter++;
            if (counter >= MAX_ROWS_PER_CHUNK) {
                break;
            }
        }
        command.getClient().tell(chunk);
        return this;
    }

}
