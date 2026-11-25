import java.nio.file.Files;
import java.nio.file.Path;
import org.opensearch.common.settings.KeyStoreWrapper;

/** Writes disposable test credentials into a shell keystore. */
public class SmokeKeystore {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected config directory");
        }
        String access = System.getenv("SMOKE_S3_ACCESS_KEY");
        String secret = System.getenv("SMOKE_S3_SECRET_KEY");
        if (access == null || secret == null) {
            throw new IllegalStateException("test credentials are required");
        }
        Path config = Path.of(args[0]);
        Files.createDirectories(config);
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.setString("s3.client.default.access_key", access.toCharArray());
        keystore.setString("s3.client.default.secret_key", secret.toCharArray());
        keystore.save(config, new char[0]);
    }
}
