package telran.monitoring.pulse;
import java.net.*;
import java.util.Arrays;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableMetadata;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import java.util.logging.*;

import org.json.JSONObject;


public class PulseReceiverAppl {

private static final String PARTITION_KEY_ATTR_NAME = "seqNumber";
private static final String PULSE_DATA_TABLE_NAME = "pulse-data";
private static final String PATIENTID_ATTR_NAME = "patientId";
private static final String TIMESTAMP_ATTR_NAME = "timestamp";
private static final String PULSE_VALUE_ATTR_NAME = "value";

private static Logger logger;

private static final DynamoDbEnhancedClient DYNAMODB_CLIENT = DynamoDbEnhancedClient.builder().build();
private static final DynamoDbTable<EnhancedDocument> DYNAMODB_TABLE = 
	DYNAMODB_CLIENT.table(PULSE_DATA_TABLE_NAME, TableSchema.documentSchemaBuilder()
			.addIndexPartitionKey(TableMetadata.primaryIndexName(), PARTITION_KEY_ATTR_NAME, AttributeValueType.N)
			.attributeConverterProviders(AttributeConverterProvider.defaultProvider())
			.build());


private static final Level LOGGING_LEVEL = parseLogLevelOrDefault("LOGGING_LEVEL", Level.INFO);
private static final int MAX_THRESHOLD_PULSE_VALUE = parseEnvarAsPositiveIntOrDefault("MAX_THRESHOLD_PULSE_VALUE", 210);
private static final int MIN_THRESHOLD_PULSE_VALUE = parseEnvarAsPositiveIntOrDefault("MIN_THRESHOLD_PULSE_VALUE", 40);
private static final int WARN_MAX_PULSE_VALUE = parseEnvarAsPositiveIntOrDefault("WARN_MAX_PULSE_VALUE", 180);
private static final int WARN_MIN_PULSE_VALUE = parseEnvarAsPositiveIntOrDefault("WARN_MAX_PULSE_VALUE", 55);

private static final int PORT = 5000;
private static final int MAX_BUFFER_SIZE = 1500;

static DatagramSocket socket;

	public static void main(String[] args) throws Exception{
		
		configureLoggingFramework();
		
		loggingServiceConfigurationValues();
		
		logger.info(String.format( "Service initialized to work with %s table of DynamoDB", PULSE_DATA_TABLE_NAME));
		socket  = new DatagramSocket(PORT);
		byte [] buffer = new byte[MAX_BUFFER_SIZE];
		while(true) {
			DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
			socket.receive(packet);
			processReceivedData(packet);
		}
	}

	private static void configureLoggingFramework() {
		logger = Logger.getLogger("logger");
		logger.setUseParentHandlers(false);
		logger.setLevel(LOGGING_LEVEL);
		Handler handler = new ConsoleHandler();
		handler.setFormatter( new Formatter() {
			
			@Override
			public String format(LogRecord record) {
				StringBuilder builder = new StringBuilder();
		        builder.append("[").append(record.getLevel()).append("] - ");
		        builder.append(record.getSourceMethodName() ).append(" - ");
		        builder.append(formatMessage(record)).append("\n");
		        return builder.toString();
			}
		});
		handler.setLevel(Level.FINEST);
		logger.addHandler(handler);
	}

	private static void loggingServiceConfigurationValues() {
		StringBuilder configInfo = new StringBuilder("Using configured values" + System.lineSeparator());
		configInfo.append(String.format("PARTITION_KEY_ATTR_NAME: %s%n", PARTITION_KEY_ATTR_NAME))
			.append(String.format("PULSE_DATA_TABLE_NAME: %s%n", PULSE_DATA_TABLE_NAME))
			.append(String.format("PORT: %d%n", PORT))
			.append(String.format("MAX_BUFFER_SIZE: %d%n", MAX_BUFFER_SIZE))
			.append(String.format("LOG_LEVEL: %s%n", LOGGING_LEVEL))
			.append(String.format("MAX_THRESHOLD_PULSE_VALUE: %d%n", MAX_THRESHOLD_PULSE_VALUE))
			.append(String.format("MIN_THRESHOLD_PULSE_VALUE: %d%n", MIN_THRESHOLD_PULSE_VALUE))
			.append(String.format("WARN_MAX_PULSE_VALUE: %d%n", WARN_MAX_PULSE_VALUE))
			.append(String.format("WARN_MIN_PULSE_VALUE: %d%n", WARN_MIN_PULSE_VALUE));
		logger.config(configInfo.toString());
	}
	
	private static void processReceivedData( DatagramPacket packet) {
		int packetLength = packet.getLength();
		logger.fine(String.format("New datagram received. Length: %d", packetLength));
		final String json = new String(Arrays.copyOf(packet.getData(), packetLength));
		
		logger.finer(() -> {
				JSONObject jsonObject = new JSONObject(json);
				return getLogEntryForPuttingInDDB(jsonObject);
			});
		logWarningAndSeverePulseCase(json);
		try {
			DYNAMODB_TABLE.putItem(EnhancedDocument.fromJson(json));
		} catch (DynamoDbException e) {
			logger.severe(String.format("Exception occured while putting new item:\n%s", e.getMessage()));
		}
	}
	


	private static void logWarningAndSeverePulseCase(String json) {
		int pulse = new JSONObject(json).optInt(PULSE_VALUE_ATTR_NAME, -1);
		if (pulse > 0 && pulse < WARN_MIN_PULSE_VALUE) {
			if (pulse >= MIN_THRESHOLD_PULSE_VALUE ) {
				logger.warning(String.format("Pulse is near low threshold: %s", json));
			} else {
				logger.severe(String.format("Low pulse threshold exeeded: %s", json));
			}
		}
		if (pulse >= WARN_MAX_PULSE_VALUE ) {
			if (pulse > MAX_THRESHOLD_PULSE_VALUE ) {
				logger.severe(String.format("High pulse threshold exeeded: %s", json));
			} else {
				logger.warning(String.format("Pulse is near high threshold: %s", json));
			}
		}
		
	}

	private static String getLogEntryForPuttingInDDB(JSONObject json) {
		return String.format("Putting new item in DynamoDb: %s=%d; %s=%d",
				PATIENTID_ATTR_NAME, json.optInt(PATIENTID_ATTR_NAME),TIMESTAMP_ATTR_NAME, json.optLong(TIMESTAMP_ATTR_NAME));
	}
	
	private static int parseEnvarAsPositiveIntOrDefault(String varName, int defaultValue) {
		int result = 0;
		try {
			result = Integer.parseInt(System.getenv(varName));
			if ( result < 0 ) {
				throw new NumberFormatException();
			}
		} catch (NumberFormatException e) {
			result = defaultValue;
		}
		return result;
	}
	private static Level parseLogLevelOrDefault( String varName, Level defaultLevel ) {
		Level logLevel = null;
		try {
			logLevel = Level.parse(System.getenv(varName));
		} catch (RuntimeException e) {
			logLevel = defaultLevel;
		}
		return logLevel;
	}

}