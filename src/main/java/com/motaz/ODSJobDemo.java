package com.motaz;

import com.motaz.dto.AccountDto;
import com.motaz.dto.CustomerAccountDto;
import com.motaz.dto.CustomerDto;
import com.motaz.dto.TransactionDto;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Properties;

public class ODSJobDemo {
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081/";
    private static final String GROUP_ID = "flink-consumer-ods-group";
    private static final String BOOTSTRAP_SERVERS_CONFIG = "PLAINTEXT_HOST://localhost:9092";
    private static final String KAFKA_TOPIC_CUSTOMER = "fulfillment.transactions_schema.T_Customers";
    private static final String KAFKA_TOPIC_ACCOUNT = "fulfillment.transactions_schema.T_Accounts";
    private static final String KAFKA_TOPIC_TRANSACTION = "fulfillment.transactions_schema.T_Transactions";

    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Configure Kafka properties
        Properties properties = getProperties();

        //Define Kafka Sources for customer, account, and transaction topics
        KafkaSource<GenericRecord> accountKafkaSource = KafkaSource.<GenericRecord>builder()
                .setProperties(properties)
                .setTopics(KAFKA_TOPIC_ACCOUNT)
                .setGroupId("flink-consumer-ods-account-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(fulfillment.transactions_schema.T_Accounts.Envelope.SCHEMA$, SCHEMA_REGISTRY_URL)).build();

        KafkaSource<GenericRecord> customerKafkaSource = KafkaSource.<GenericRecord>builder()
                .setProperties(properties)
                .setTopics(KAFKA_TOPIC_CUSTOMER)
                .setGroupId("flink-consumer-ods-customer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(fulfillment.transactions_schema.T_Customers.Envelope.SCHEMA$, SCHEMA_REGISTRY_URL)).build();

        KafkaSource<GenericRecord> transactionKafkaSource = KafkaSource.<GenericRecord>builder()
                .setProperties(properties)
                .setTopics(KAFKA_TOPIC_TRANSACTION)
                .setGroupId("flink-consumer-ods-transaction-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(fulfillment.transactions_schema.T_Transactions.Envelope.SCHEMA$, SCHEMA_REGISTRY_URL)).build();

        //define waterMark
        WatermarkStrategy<GenericRecord> watermarkStrategy =
                WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(Duration.ofSeconds(20)) // Adjust the duration as needed
                .withTimestampAssigner((event, timestamp) -> getWaterMarkStrategy(event));

        // Add sources to the environment
        DataStream<CustomerDto> customerStream = env.fromSource(customerKafkaSource, watermarkStrategy, "Customer Source")
                .map(new MapFunction<GenericRecord, CustomerDto>() {
                    @Override
                    public CustomerDto map(GenericRecord genericRecord) throws Exception {
                        return mapToCustomerDto(genericRecord);
                    }
                });

        DataStream<AccountDto> accountStream = env.fromSource(accountKafkaSource, watermarkStrategy, "Account Source")
                .map(new MapFunction<GenericRecord, AccountDto>() {
                    @Override
                    public AccountDto map(GenericRecord genericRecord) throws Exception {
                        return mapToAccountDto(genericRecord);
                    }
                });

        DataStream<TransactionDto> transactionStream = env.fromSource(transactionKafkaSource, watermarkStrategy, "Transaction Source")
                .map(new MapFunction<GenericRecord, TransactionDto>() {
                    @Override
                    public TransactionDto map(GenericRecord genericRecord) throws Exception {
                        return mapToTransactionDto(genericRecord);
                    }
                });

        DataStream<CustomerDto> customerStreamByKey = customerStream.keyBy(CustomerDto::getId);
        DataStream<AccountDto> accountStreamByKey = accountStream.keyBy(AccountDto::getCustomerId);

        DataStream<CustomerAccountDto> denormalizedStream = customerStreamByKey
                .connect(accountStreamByKey)
                .process(new CustomerAccountCoProcessFunction());

        // Now you can print or write the denormalized stream to a sink
        denormalizedStream.print("denormalizedStream>> ");

        // Define the SQL query for inserting data into the denormalized customer-account table
        String customerAccountSql = "Insert into customer_account (customer_id, customer_name, email, mobile, account_id, account_number, account_status, account_balance, updated_at) values (?,?,?,?,?,?,?,?,?) \n" +
                "ON CONFLICT (account_number) DO UPDATE " +
                "SET " +
                "    customer_name = EXCLUDED.customer_name,\n" +
                "    email = EXCLUDED.email,\n" +
                "    mobile = EXCLUDED.mobile,\n" +
                "    account_status = EXCLUDED.account_status,\n" +
                "    account_balance = EXCLUDED.account_balance";

        // Create the sink for the denormalized customer-account table
        SinkFunction<CustomerAccountDto> customerAccountSink = createJdbcSink(
                customerAccountSql,
                (statement, denormalizedCustomerAccount) -> {
                    // Set parameters from the denormalized customer account object
                    statement.setInt(1,denormalizedCustomerAccount.getCustomerId());
                    statement.setString(2,denormalizedCustomerAccount.getCustomerName());
                    statement.setString(3,denormalizedCustomerAccount.getEmail());
                    statement.setString(4,denormalizedCustomerAccount.getMobile());
                    statement.setInt(5, denormalizedCustomerAccount.getAccountId());
                    statement.setString(6,denormalizedCustomerAccount.getAccountNumber());
                    statement.setString(7,denormalizedCustomerAccount.getAccountStatus());
                    statement.setString(8,denormalizedCustomerAccount.getAccountBalance());
                    statement.setTimestamp(9, Timestamp.valueOf(denormalizedCustomerAccount.getUpdatedAt()));
                });
        denormalizedStream.addSink(customerAccountSink);



        // Define the SQL query for inserting data into the Transaction table
        String transactionSql = "Insert into t_transaction (transaction_id, transaction_ref_number, transaction_amount, transaction_type, account_number, transaction_date) " +
                " values (?,?,?,?,?,?) " ;
        // Create the sink for the denormalized customer-account table
        SinkFunction<TransactionDto> transactionSink = createJdbcSink(
                transactionSql,
                (statement, transactionDto) -> {
                    // Set parameters from the denormalized customer account object
                    statement.setInt(1,transactionDto.getId());
                    statement.setString(2,transactionDto.getTransactionRefNumber());
                    statement.setString(3,String.valueOf(transactionDto.getTransactionAmount()));
                    statement.setString(4,transactionDto.getTransactionType());
                    statement.setInt(5, transactionDto.getAccountId());
                    statement.setTimestamp(6,Timestamp.valueOf(transactionDto.getCreatedAt()));
                });
        transactionStream.addSink(transactionSink);

        // Execute the job
        env.execute("Flink Multiple Kafka Sources");

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return props;
    }

    private static AccountDto mapToAccountDto(GenericRecord genericRecord) {
        return Optional.ofNullable(genericRecord.get("after"))
                .map(afterRecord -> (GenericRecord) afterRecord)
                .map(after -> AccountDto.builder()
                        .id(getIntValue(after, "id"))
                        .accountNumber(getIntValue(after, "account_number"))
                        .accountStatus(getStringValue(after, "account_status"))
                        .accountBalance(getStringValue(after, "account_balance"))
                        .customerId(getIntValue(after, "customer_id"))
                        .createdAt(convertToLocalDateTime(getLongValue(after, "created_at")))
                        .updatedAt(convertToLocalDateTime(getLongValue(after, "updated_at")))
                        .build())
                .orElse(null);
    }

    private static CustomerDto mapToCustomerDto(GenericRecord genericRecord) {
        return Optional.ofNullable(genericRecord.get("after"))
                .map(afterRecord -> (GenericRecord) afterRecord)
                .map(after -> CustomerDto.builder()
                        .id(getIntValue(after, "id"))
                        .firstName(getStringValue(after, "first_name"))
                        .lastName(getStringValue(after, "last_name"))
                        .email(getStringValue(after, "email"))
                        .mobile(getStringValue(after, "mobile"))
                        .createdAt(convertToLocalDateTime(getLongValue(after, "created_at")))
                        .updatedAt(convertToLocalDateTime(getLongValue(after, "updated_at")))
                        .build())
                .orElse(null);
    }

    private static TransactionDto mapToTransactionDto(GenericRecord genericRecord) {
        return Optional.ofNullable(genericRecord.get("after"))
                .map(afterRecord -> (GenericRecord) afterRecord)
                .map(after -> TransactionDto.builder()
                        .id(getIntValue(after, "id"))
                        .transactionRefNumber(getStringValue(after, "transaction_ref_number"))
                        .customerId(getIntValue(after, "customer_id"))
                        .accountId(getIntValue(after, "account_id"))
                        .transactionAmount(getIntValue(after, "transaction_amount"))
                        .transactionType(getStringValue(after, "transaction_type"))
                        .createdAt(convertToLocalDateTime(getLongValue(after, "created_at")))
                        .build())
                .orElse(null);
    }

    private static String getStringValue(GenericRecord record, String fieldName) {
        return Optional.ofNullable(record.get(fieldName)).map(Object::toString).orElse(null);
    }

    private static Integer getIntValue(GenericRecord record, String fieldName) {
        return Optional.ofNullable(record.get(fieldName)).map(Object::toString).map(Integer::valueOf).orElse(null);
    }

    private static Long getLongValue(GenericRecord record, String fieldName) {
        return Optional.ofNullable(record.get(fieldName)).map(Object::toString).map(Long::valueOf).orElse(null);
    }

    private static LocalDateTime convertToLocalDateTime(long input) {
        // convert to milliseconds
        long millisecondTimestamp = input / 1000;
        // Create an Instant
        Instant instant = Instant.ofEpochMilli(millisecondTimestamp);
        // add extra microseconds precision to the Instant
        long microseconds = input % 1000;
        instant = instant.plusNanos(microseconds * 1000);
        // Convert Instant to LocalDateTime via a ZonedDateTime
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    public static long getWaterMarkStrategy(GenericRecord genericRecord) {
        long time = Optional.ofNullable(genericRecord.get("source"))
                .map(sourceRecord -> (GenericRecord) sourceRecord)
                .map(source -> getLongValue(source, "ts_ms"))
                .orElse(System.currentTimeMillis());
        return time;
    }

    public static <T> SinkFunction<T> createJdbcSink(
            String sqlQuery,
            JdbcStatementBuilder<T> statementBuilder) {

        return JdbcSink.sink(
                sqlQuery,
                statementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/Transaction?currentSchema=ods_schema")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("postgres")
                        .withPassword("postgres")
                        .build());
    }

}
