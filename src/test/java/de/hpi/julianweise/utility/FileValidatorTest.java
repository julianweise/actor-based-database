package de.hpi.julianweise.utility;

import com.beust.jcommander.ParameterException;
import de.hpi.julianweise.utility.file.FileValidator;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;

public class FileValidatorTest {

    @ClassRule
    public final static TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public final ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testThrowsExceptionFileNotFound() {
        expectedEx.expect(ParameterException.class);
        expectedEx.expectMessage("the given TestFile.csv does not exist");

        FileValidator validatorUnderTest = new FileValidator();
        validatorUnderTest.validate("TestFile.csv", Paths.get("invalid path"));
    }

    @Test
    public void testThrowsExceptionFileWrongType() throws IOException {
        expectedEx.expect(ParameterException.class);
        expectedEx.expectMessage("the given abc is not a file");

        FileValidator validatorUnderTest = new FileValidator();
        validatorUnderTest.validate("abc", folder.newFolder("abc").toPath());
    }
}
