package de.hpi.julianweise.settings;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class SettingsTest {

    @ClassRule
    public final static TemporaryFolder folder = new TemporaryFolder();

    @After
    public void after() {
        folder.delete();
    }

    @Test
    public void expectSuccessfulConfigParsing() {
        String config =
                "actor-db.csv.chunk-size = 5\n" +
                "actor-db.query-endpoint.hostname = localhost \n" +
                "actor-db.number-of-threads = 4\n" +
                "actor-db.query-endpoint.port = 2020";
        TestKitJunitResource testKit = new TestKitJunitResource(config);

        SettingsImpl settings = Settings.SettingsProvider.get(testKit.system());

        assertThat(settings.CSV_CHUNK_SIZE).isEqualTo(5);
    }

    @Test
    public void lookupSingelton() {
        Settings settings = Settings.SettingsProvider;
        assertThat(settings.lookup()).isEqualTo(settings);
    }
}
