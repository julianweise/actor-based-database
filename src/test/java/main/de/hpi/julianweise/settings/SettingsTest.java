package main.de.hpi.julianweise.settings;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class SettingsTest {
    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @After
    public void after() {
        folder.delete();
    }

    @Test
    public void expectSuccessfulConfigParsing() {
        TestKitJunitResource testKit = new TestKitJunitResource("actor-db.csv.chunk-size = 5");

        SettingsImpl settings = Settings.SettingsProvider.get(testKit.system());

        assertThat(settings.CSV_CHUNK_SIZE).isEqualTo(5);
    }
}
