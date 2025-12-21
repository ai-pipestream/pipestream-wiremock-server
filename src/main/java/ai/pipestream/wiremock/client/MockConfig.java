package ai.pipestream.wiremock.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration utility for service mocks.
 * <p>
 * Supports loading configuration from:
 * <ol>
 *   <li>Environment variables (highest priority)</li>
 *   <li>Config file: {@code wiremock-mocks.properties} in the classpath or current directory</li>
 *   <li>System properties</li>
 * </ol>
 * <p>
 * Configuration keys follow the pattern: {@code wiremock.<service>.<method>.<property>}
 * <p>
 * Example for AccountService:
 * <pre>
 * wiremock.account.GetAccount.default.id=default-account
 * wiremock.account.GetAccount.default.name=Default Account
 * wiremock.account.GetAccount.default.description=Default account for testing
 * wiremock.account.GetAccount.default.active=true
 * </pre>
 */
public class MockConfig {
    
    private static final Logger LOG = LoggerFactory.getLogger(MockConfig.class);
    
    private static final String CONFIG_FILE = "wiremock-mocks.properties";
    private final Properties properties;
    
    /**
     * Creates a new MockConfig instance and loads configuration from all sources.
     */
    public MockConfig() {
        this.properties = new Properties();
        loadConfig();
    }
    
    /**
     * Loads configuration from environment variables, config file, and system properties.
     */
    private void loadConfig() {
        // First, try to load from config file
        loadFromFile();
        
        // Then, override with system properties
        loadFromSystemProperties();
        
        // Finally, override with environment variables (highest priority)
        loadFromEnvironment();
        
        if (properties.isEmpty()) {
            LOG.debug("No mock configuration found. Using defaults.");
        } else {
            LOG.info("Loaded {} mock configuration property(ies)", properties.size());
        }
    }
    
    /**
     * Loads configuration from the config file.
     */
    private void loadFromFile() {
        // Try classpath first
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is != null) {
                properties.load(is);
                LOG.debug("Loaded configuration from classpath: {}", CONFIG_FILE);
                return;
            }
        } catch (IOException e) {
            LOG.debug("Could not load config file from classpath: {}", e.getMessage());
        }
        
        // Try current directory
        try (FileInputStream fis = new FileInputStream(CONFIG_FILE)) {
            properties.load(fis);
            LOG.debug("Loaded configuration from current directory: {}", CONFIG_FILE);
        } catch (IOException e) {
            LOG.debug("Config file not found in current directory: {}", CONFIG_FILE);
        }
    }
    
    /**
     * Loads configuration from system properties.
     */
    private void loadFromSystemProperties() {
        System.getProperties().stringPropertyNames().stream()
                .filter(key -> key.startsWith("wiremock."))
                .forEach(key -> properties.setProperty(key, System.getProperty(key)));
    }
    
    /**
     * Loads configuration from environment variables.
     * <p>
     * Environment variable names are converted from UPPER_SNAKE_CASE to lowercase dot notation.
     * Example: {@code WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ID} → {@code wiremock.account.GetAccount.default.id}
     */
    private void loadFromEnvironment() {
        System.getenv().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("WIREMOCK_"))
                .forEach(entry -> {
                    String key = convertEnvKeyToPropertyKey(entry.getKey());
                    properties.setProperty(key, entry.getValue());
                });
    }
    
    /**
     * Converts an environment variable key to a property key.
     * <p>
     * Example: {@code WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ID} → {@code wiremock.account.GetAccount.default.id}
     */
    private String convertEnvKeyToPropertyKey(String envKey) {
        // Remove WIREMOCK_ prefix and convert to lowercase
        String withoutPrefix = envKey.substring("WIREMOCK_".length()).toLowerCase();
        // Replace underscores with dots
        return "wiremock." + withoutPrefix.replace("_", ".");
    }
    
    /**
     * Gets a configuration value, returning the default if not found.
     *
     * @param key The configuration key (e.g., "wiremock.account.GetAccount.default.id")
     * @param defaultValue The default value to return if key is not found
     * @return The configuration value or default
     */
    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
    
    /**
     * Gets a configuration value as a boolean.
     *
     * @param key The configuration key
     * @param defaultValue The default value
     * @return The boolean value
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
    
    /**
     * Gets a configuration value as an integer.
     *
     * @param key The configuration key
     * @param defaultValue The default value
     * @return The integer value
     */
    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid integer value for key '{}': {}. Using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }
    
    /**
     * Checks if a configuration key exists.
     *
     * @param key The configuration key
     * @return true if the key exists
     */
    public boolean hasKey(String key) {
        return properties.containsKey(key);
    }
}




