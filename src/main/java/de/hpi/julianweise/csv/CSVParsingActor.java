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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class CSVParsingActor extends AbstractBehavior<CSVParsingActor.Command> {

    private final InputStreamReader inputStreamReader;
    private final Iterator<CSVRecord> csvIterator;
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
        this.inputStreamReader = new InputStreamReader(this.locateCSVFile(filePath));
        CSVParser csvParser = this.openCSVForParsing();
        this.csvIterator = csvParser.iterator();
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ParseNextCSVChunk.class, this::handleParseNextCSVChunk)
                .build();
    }

    @SneakyThrows(FileNotFoundException.class)
    private FileInputStream locateCSVFile(String csvPath) {
        File csvFile = new File(csvPath);
        return new FileInputStream(csvFile);
    }

    @SneakyThrows
    private CSVParser openCSVForParsing() {
        return CSVFormat.EXCEL.withFirstRecordAsHeader().parse(this.inputStreamReader);
    }

    private Behavior<Command> handleParseNextCSVChunk(ParseNextCSVChunk command) {
        if (!this.csvIterator.hasNext()) {
            command.getClient().tell(new CSVFullyParsed());
            return Behaviors.same();
        }
        int counter = 0;
        ObjectList<CSVRecord> chunk = new ObjectArrayList<>(this.settings.CSV_CHUNK_SIZE);

        while (csvIterator.hasNext() && counter < this.settings.CSV_CHUNK_SIZE) {
            chunk.add(this.csvIterator.next());
        }

        command.getClient().tell(new CSVDataChunk(chunk));
        return Behaviors.same();
    }
}
