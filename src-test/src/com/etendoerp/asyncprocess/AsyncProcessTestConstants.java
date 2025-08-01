package com.etendoerp.asyncprocess;

/**
 * Constants used in the AsyncProcess tests.
 */
public class AsyncProcessTestConstants {
  public static final String MESSAGE_KEY = "message";
  public static final String MESSAGE_VALUE = "Test message";

  public static final String CLIENT_ID_KEY = "testClientId";
  public static final String CLIENT_ID_VALUE = "client_id";
  public static final String ASSERT_SHOULD_RETURN_MOCKED_RESULT = "Should return the mocked result";
  public static final String ASSERT_RESULT_NOT_NULL = "Result should not be null";

  public static final String ORG_ID_KEY = "org_id";
  public static final String ORG_ID_VALUE = "testOrgId";

  public static final String JOB_ID_KEY = "jobs_job_id";
  public static final String JOB_ID_VALUE = "testJobId";

  public static final String JOB_SCHEDULERS = "jobSchedulers";
  public static final String JOB_PARTITION_ID = "job-123";

  public static final String KAFKA_URL_KEY = "kafka.url";
  public static final String KAFKA_URL_VALUE = "localhost:29092";

  public static final String KAFKA_ENABLE_KEY = "kafka.enable";
  public static final String KAFKA_ENABLE_VALUE = "false";

  public static final String KAFKA_PARTITIONS_KEY = "kafka.topic.partitions";

  public static final String GET_NUM_PARTITIONS_METHOD = "getNumPartitions";
  public static final String GET_KAFKA_HOST_METHOD = "getKafkaHost";

  public static final String ETAP_PARALLEL_THREADS = "etapParallelThreads";

  public static final String DOCKER_TOMCAT_NAME = "docker_com.etendoerp.tomcat";

  public static final String TEST_DATA = "test data";
  public static final String TEST_TOPIC = "test-topic";
  public static final String EXTRACT_TARGETS_METHOD = "extractTargetsFromResult";

  public static final String TEST_PROCESS_ID = "test-process-id";
  public static final String TEST_ERROR_MESSAGE = "Test error";

  /**
   * Private constructor to prevent instantiation.
   * Throws an exception if called, even via reflection.
   */
  private AsyncProcessTestConstants() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }
}
