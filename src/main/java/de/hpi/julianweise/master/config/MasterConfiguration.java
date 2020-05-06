package de.hpi.julianweise.master.config;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.julianweise.utility.config.ConfigurationBase;
import de.hpi.julianweise.utility.file.FileValidator;
import lombok.Getter;
import lombok.Setter;

import java.nio.file.Path;
import java.nio.file.Paths;

@Parameters(commandDescription = "start a master actor system")
@Getter
@Setter
public class MasterConfiguration implements ConfigurationBase {

    @Parameter(names = {"-i", "--input"}, description = "data to handle", validateValueWith = FileValidator.class,
               converter = StringToPathConverter.class)
    private Path inputFile;

    @Parameter(names = {"-p", "--port"}, description = "port to run application on")
    private int port;

    @Parameter(names = {"-d", "--data-loading-strategy"}, description = "fully-qualified class-name of loading " +
            "strategy")
    private String dataLoadingStrategy;

    private static class StringToPathConverter implements IStringConverter<Path> {
        @Override
        public Path convert(String path) {
            return Paths.get(path);
        }

    }

    public OperationRole role() {
        return OperationRole.MASTER;
    }
}
