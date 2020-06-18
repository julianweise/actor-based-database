package de.hpi.julianweise.csv;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class CSVParsingActor extends AbstractBehavior<CSVParsingActor.Command> {

    private final BufferedReader inputReader;
    private final Iterable<CSVRecord> csvIterator;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());

    public interface Command {}

    public interface Response {}

    @AllArgsConstructor
    @Getter
    public static class ParseNextCSVChunk implements CSVParsingActor.Command {
        private final ActorRef<Response> client;
    }

    @Getter
    @AllArgsConstructor
    public static class CSVDataChunk implements Response {
        private final ObjectList<CSVRecord> chunk;
    }

    @Getter
    @AllArgsConstructor
    public static class CSVFullyParsed implements Response {}

    protected CSVParsingActor(ActorContext<CSVParsingActor.Command> context, String filePath) {
        super(context);
        this.inputReader = new BufferedReader(this.locateCSVFile(filePath));
        this.csvIterator = this.openCSVForParsing();
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ParseNextCSVChunk.class, this::handleParseNextCSVChunk)
                .build();
    }

    @SneakyThrows(FileNotFoundException.class)
    private FileReader locateCSVFile(String csvPath) {
        File csvFile = new File(csvPath);
        return new FileReader(csvFile);
    }

    @SneakyThrows
    private CSVParser openCSVForParsing() {
        return CSVFormat.RFC4180.withFirstRecordAsHeader().parse(this.inputReader);
    }

    private Behavior<Command> handleParseNextCSVChunk(ParseNextCSVChunk command) {
        int counter = 0;
        ObjectList<CSVRecord> chunk = new ObjectArrayList<>(this.settings.CSV_CHUNK_SIZE);

        for (CSVRecord line: this.csvIterator) {
            chunk.add(line);
            if (++counter >= this.settings.CSV_CHUNK_SIZE) {
                command.getClient().tell(new CSVDataChunk(chunk));
                return Behaviors.same();
            }
        }

        command.getClient().tell(new CSVFullyParsed());
        return Behaviors.same();
    }
}
