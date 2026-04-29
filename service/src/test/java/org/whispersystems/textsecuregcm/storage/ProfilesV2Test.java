/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class ProfilesV2Test {
  private static final UUID ACI = UUID.randomUUID();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.PROFILES_V2);

  private ProfilesV2 profilesV2;
  private byte[] version;
  private byte[] data;
  private byte[] commitment;
  private byte[] paymentAddress;

  private VersionedProfile profile;

  @BeforeEach
  void setUp() throws InvalidInputException {
    profilesV2 = new ProfilesV2(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.PROFILES_V2.tableName());

    commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(ACI)).serialize();
    version = TestRandomUtil.nextBytes(32);
    data = TestRandomUtil.nextBytes(256);
    paymentAddress = TestRandomUtil.nextBytes(582);

    profile = new VersionedProfile(version, data, paymentAddress, commitment);
  }

  /// Generates a no-op transaction write item for tests that don’t need a real one
  private TransactWriteItem getAdditionalTransactWriteItem() {
    return TransactWriteItem.builder()
        .conditionCheck(builder -> builder
            .tableName(Tables.PROFILES_V2.tableName())
            .key(Map.of(
                ProfilesV2.KEY_ACCOUNT_UUID, AttributeValues.b(UUID.randomUUID()),
                ProfilesV2.KEY_VERSION, AttributeValue.builder().b(SdkBytes.fromByteArray("not-a-real-version".getBytes())).build()))
            .conditionExpression("attribute_not_exists(#uuid)")
            .expressionAttributeNames(Map.of("#uuid", ProfilesV2.KEY_ACCOUNT_UUID)))
        .build();
  }

  @Test
  void testSetFirstWrite() throws WriteConflictException {
    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(), null, getAdditionalTransactWriteItem());

    final Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(data);
    assertThat(retrieved.get().commitment()).isEqualTo(commitment);
    assertThat(retrieved.get().paymentAddress()).isEqualTo(paymentAddress);
    assertThat(retrieved.get().dataHash()).isNotNull();
    assertThat(retrieved.get().paymentAddressHash()).isNotNull();
  }

  @Test
  void testSetWithoutPaymentAddress() throws WriteConflictException {

    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), null, null, null, getAdditionalTransactWriteItem());

    final Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(data);
    assertThat(retrieved.get().commitment()).isEqualTo(commitment);
    assertThat(retrieved.get().paymentAddress()).isNull();
    assertThat(retrieved.get().paymentAddressHash()).isNull();
  }

  @Test
  void testInitialSetRequiresCommitment() {
    assertThrows(IllegalArgumentException.class, () -> {
      profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), null, profile.paymentAddress(), profile.paymentAddressHash(), null, getAdditionalTransactWriteItem());
    });
  }

  @Test
  void testConditionalUpdateSuccess() throws WriteConflictException {

    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(), null, getAdditionalTransactWriteItem());

    Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();

    // Update with correct expected hash
    final byte[] newData = TestRandomUtil.nextBytes(256);
    profilesV2.set(ACI, profile.version(), newData, VersionedProfile.hash(newData), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(), retrieved.get().dataHash(), getAdditionalTransactWriteItem());

    retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(newData);
  }

  @Test
  void testConditionalUpdateFailure() throws WriteConflictException {

    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(),null, getAdditionalTransactWriteItem());

    final byte[] wrongHash = TestRandomUtil.nextBytes(32);

    assertThrows(WriteConflictException.class, () -> {
      profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(), wrongHash, getAdditionalTransactWriteItem());
    });

    // Verify data is unchanged
    final Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(data);
  }

  @Test
  void testCommitmentImmutability() throws InvalidInputException, WriteConflictException {

    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(),null, getAdditionalTransactWriteItem());

    Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();

    // Try to update with a different commitment
    final byte[] newData = TestRandomUtil.nextBytes(256);
    final byte[] differentCommitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    assertThrows(IllegalArgumentException.class,
        () -> profilesV2.set(ACI, profile.version(), newData, VersionedProfile.hash(newData), differentCommitment, profile.paymentAddress(), profile.paymentAddressHash(), retrieved.get().dataHash(),
            getAdditionalTransactWriteItem()));
  }

  @Test
  void testUpdateWithNullCommitmentSucceeds() throws WriteConflictException {
    profilesV2.set(ACI,profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(),null, getAdditionalTransactWriteItem());

    Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();
    final byte[] originalCommitment = retrieved.get().commitment();

    final byte[] newData = TestRandomUtil.nextBytes(256);
    profilesV2.set(ACI, profile.version(), newData, VersionedProfile.hash(newData), null, profile.paymentAddress(), profile.paymentAddressHash(), retrieved.get().dataHash(), getAdditionalTransactWriteItem());

    retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(newData);
    assertThat(retrieved.get().commitment()).isEqualTo(originalCommitment);
  }

  @Test
  void testMultipleVersions() throws InvalidInputException, WriteConflictException {
    final byte[] versionOne = TestRandomUtil.nextBytes(32);
    final byte[] versionTwo = TestRandomUtil.nextBytes(32);

    final byte[] dataOne = TestRandomUtil.nextBytes(256);
    final byte[] dataTwo = TestRandomUtil.nextBytes(256);

    final byte[] commitmentOne = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentTwo = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    profilesV2.set(ACI, versionOne, dataOne, VersionedProfile.hash(dataOne), commitmentOne, paymentAddress,
        VersionedProfile.hash(paymentAddress), null, getAdditionalTransactWriteItem());
    profilesV2.set(ACI, versionTwo, dataTwo, VersionedProfile.hash(dataTwo), commitmentTwo, null, null,null, getAdditionalTransactWriteItem());

    Optional<VersionedProfile> retrieved = profilesV2.get(ACI, versionOne);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(dataOne);
    assertThat(retrieved.get().commitment()).isEqualTo(commitmentOne);
    assertThat(retrieved.get().paymentAddress()).isEqualTo(paymentAddress);

    retrieved = profilesV2.get(ACI, versionTwo);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(dataTwo);
    assertThat(retrieved.get().commitment()).isEqualTo(commitmentTwo);
    assertThat(retrieved.get().paymentAddress()).isNull();
  }

  @Test
  void testMissing() throws WriteConflictException {
    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(),null, getAdditionalTransactWriteItem());
    final byte[] missingVersion = TestRandomUtil.nextBytes(32);

    final Optional<VersionedProfile> retrieved = profilesV2.get(ACI, missingVersion);
    assertThat(retrieved.isPresent()).isFalse();
  }

  @Test
  void testDeleteAll() throws InvalidInputException, WriteConflictException {
    final byte[] versionOne = TestRandomUtil.nextBytes(32);
    final byte[] versionTwo = TestRandomUtil.nextBytes(32);
    final byte[] versionThree = TestRandomUtil.nextBytes(32);

    final byte[] dataOne = TestRandomUtil.nextBytes(256);
    final byte[] dataTwo = TestRandomUtil.nextBytes(256);
    final byte[] dataThree = TestRandomUtil.nextBytes(256);

    final byte[] commitmentOne = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentTwo = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentThree = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    profilesV2.set(ACI, versionOne, dataOne, VersionedProfile.hash(dataOne), commitmentOne, paymentAddress, VersionedProfile.hash(paymentAddress), null, getAdditionalTransactWriteItem());
    profilesV2.set(ACI, versionTwo, dataTwo, VersionedProfile.hash(dataTwo), commitmentTwo, paymentAddress, VersionedProfile.hash(paymentAddress), null, getAdditionalTransactWriteItem());
    profilesV2.set(ACI, versionThree, dataThree, VersionedProfile.hash(dataThree), commitmentThree, null, null,null, getAdditionalTransactWriteItem());

    profilesV2.deleteAll(ACI).join();

    for (byte[] v : List.of(versionOne, versionTwo, versionThree)) {
      final Optional<VersionedProfile> retrieved = profilesV2.get(ACI, v);
      assertThat(retrieved.isPresent()).isFalse();
    }
  }

  @Test
  void testUpdatePaymentAddress() throws WriteConflictException {

    profilesV2.set(ACI, profile.version(), profile.data(), profile.dataHash(), profile.commitment(), profile.paymentAddress(), profile.paymentAddressHash(),null, getAdditionalTransactWriteItem());

    Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().paymentAddress()).isEqualTo(paymentAddress);

    // Update to remove payment address
    final byte[] newData = TestRandomUtil.nextBytes(256);
    profilesV2.set(ACI, version, newData, VersionedProfile.hash(newData), commitment, null, null, retrieved.get().dataHash(), getAdditionalTransactWriteItem());

    retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().data()).isEqualTo(newData);
    assertThat(retrieved.get().paymentAddress()).isNull();
    assertThat(retrieved.get().paymentAddressHash()).isNull();
  }

  @Test
  void testSetWithAdditionalTransactWriteItemConflict() throws WriteConflictException {

    profilesV2.set(ACI, version, data, VersionedProfile.hash(data), commitment, paymentAddress, VersionedProfile.hash(paymentAddress), null, getAdditionalTransactWriteItem());

    final Optional<VersionedProfile> retrieved = profilesV2.get(ACI, version);
    assertThat(retrieved.isPresent()).isTrue();

    // write an update
    final byte[] updatedData = TestRandomUtil.nextBytes(256);
    profilesV2.set(ACI, version, updatedData, VersionedProfile.hash(updatedData), null, paymentAddress, VersionedProfile.hash(paymentAddress), retrieved.get().dataHash(), getAdditionalTransactWriteItem());

    // attempt a second update with a stale hash
    assertThrows(WriteConflictException.class, () -> {
      final byte[] conflictedData = TestRandomUtil.nextBytes(256);
      profilesV2.set(ACI, version, conflictedData, VersionedProfile.hash(conflictedData), null, paymentAddress, VersionedProfile.hash(paymentAddress), retrieved.get().dataHash(), getAdditionalTransactWriteItem());
    });

    // still has updatedData
    final Optional<VersionedProfile> retrievedV2 = profilesV2.get(ACI, version);
    assertThat(retrievedV2.isPresent()).isTrue();
    assertThat(retrievedV2.get().data()).isEqualTo(updatedData);
  }

  @CartesianTest
  void testGetTransactWriteItem(
      @CartesianTest.Values(booleans = {true, false}) final boolean hasCommitment,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasExpectedDataHash) {
    final byte[] commitment = hasCommitment ? profile.commitment() : null;
    final byte[] expectedDataHash = hasExpectedDataHash ? TestRandomUtil.nextBytes(32) : null;

    final TransactWriteItem transactWriteItem = profilesV2.getTransactWriteItem(
        ACI, profile.version(), profile.data(), profile.dataHash(), commitment, profile.paymentAddress(), profile.paymentAddressHash(), expectedDataHash);

    assertThat(transactWriteItem).isNotNull();
    assertThat(transactWriteItem.update()).isNotNull();
    assertThat(transactWriteItem.update().tableName()).isEqualTo(Tables.PROFILES_V2.tableName());
    assertThat(transactWriteItem.update().key()).isNotNull();
    assertThat(transactWriteItem.update().updateExpression()).isEqualTo(ProfilesV2.buildUpdateExpression(commitment != null, paymentAddress != null));
    assertThat(transactWriteItem.update().conditionExpression()).isEqualTo(ProfilesV2.buildConditionExpression(commitment != null, expectedDataHash != null));
    assertThat(transactWriteItem.update().expressionAttributeValues())
        .isEqualTo(ProfilesV2.buildUpdateExpressionAttributeValues(data, profile.dataHash(), commitment, paymentAddress, profile.paymentAddressHash(), expectedDataHash));
  }

  @ParameterizedTest
  @MethodSource
  void testBuildUpdateExpression(final byte[] commitment, final byte[] paymentAddress, final String expectedUpdateExpression) {
    assertEquals(expectedUpdateExpression, ProfilesV2.buildUpdateExpression(commitment != null, paymentAddress != null));
  }

  private static Stream<Arguments> testBuildUpdateExpression() {
    final byte[] commitment = TestRandomUtil.nextBytes(32);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);

    return Stream.of(
        Arguments.argumentSet("with commitment and payment address",
            commitment,
            paymentAddress,
            "SET #data = :data, #dataHash = :dataHash, #commitment = if_not_exists(#commitment, :commitment), #paymentAddress = :paymentAddress, #paymentAddressHash = :paymentAddressHash"),
        Arguments.argumentSet("with commitment, without payment address",
            commitment,
            null,
            "SET #data = :data, #dataHash = :dataHash, #commitment = if_not_exists(#commitment, :commitment) REMOVE #paymentAddress, #paymentAddressHash"),
        Arguments.argumentSet("without commitment, with payment address",
            null,
            paymentAddress,
            "SET #data = :data, #dataHash = :dataHash, #paymentAddress = :paymentAddress, #paymentAddressHash = :paymentAddressHash"),
        Arguments.argumentSet("without commitment or payment address",
            null,
            null,
            "SET #data = :data, #dataHash = :dataHash REMOVE #paymentAddress, #paymentAddressHash")
    );
  }

  @ParameterizedTest
  @MethodSource
  void testBuildConditionExpression(final byte[] commitment, final byte[] expectedCurrentDataHash, final String expectedConditionExpression) {
    assertEquals(expectedConditionExpression, ProfilesV2.buildConditionExpression(commitment != null, expectedCurrentDataHash != null));
  }

  private static Stream<Arguments> testBuildConditionExpression() {
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] dataHash = TestRandomUtil.nextBytes(32);

    return Stream.of(
        Arguments.argumentSet("initial write",
            commitment,
            null,
            "(attribute_not_exists(#commitment) OR #commitment = :commitment) AND attribute_not_exists(#dataHash)"),
        Arguments.argumentSet("update with commitment",
            commitment,
            dataHash,
            "(attribute_not_exists(#commitment) OR #commitment = :commitment) AND #dataHash = :expectedDataHash"),
        Arguments.argumentSet("update without commitment",
            null,
            dataHash,
            "size(#commitment) = :commitment_length AND #dataHash = :expectedDataHash")
    );
  }

  @ParameterizedTest
  @MethodSource
  void testBuildUpdateExpressionAttributeValues(
      final byte[] data,
      final byte[] dataHash,
      final byte[] commitment,
      final byte[] paymentAddress,
      final byte[] paymentAddressHash,
      final byte[] expectedCurrentDataHash,
      final int expectedMapSize) {

    final Map<String, AttributeValue> result = ProfilesV2.buildUpdateExpressionAttributeValues(
        data, dataHash, commitment, paymentAddress, paymentAddressHash, expectedCurrentDataHash);

    assertEquals(expectedMapSize, result.size());
    assertThat(result.containsKey(":data")).isTrue();
    assertThat(result.containsKey(":dataHash")).isTrue();

    if (commitment != null) {
      assertThat(result.containsKey(":commitment")).isTrue();
    } else {
      assertThat(result.containsKey(":commitment_length")).isTrue();
    }

    if (expectedCurrentDataHash != null) {
      assertThat(result.containsKey(":expectedDataHash")).isTrue();
    }

    if (paymentAddress != null) {
      assertThat(result.containsKey(":paymentAddress")).isTrue();
      assertThat(result.containsKey(":paymentAddressHash")).isTrue();
    }

  }

  private static Stream<Arguments> testBuildUpdateExpressionAttributeValues() throws InvalidInputException {
    final byte[] data = TestRandomUtil.nextBytes(256);
    final byte[] commitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] dataHash = TestRandomUtil.nextBytes(32);

    return Stream.of(
        Arguments.argumentSet("all fields present",
            data, VersionedProfile.hash(data), commitment, paymentAddress, VersionedProfile.hash(paymentAddress), dataHash, 6),
        Arguments.argumentSet("no payment address",
            data, VersionedProfile.hash(data), commitment, null, null, dataHash, 4),
        Arguments.argumentSet("no commitment",
            data, VersionedProfile.hash(data), null, paymentAddress, VersionedProfile.hash(paymentAddress), dataHash, 6),
        Arguments.argumentSet("only data fields",
            data, VersionedProfile.hash(data), null, null, null, dataHash, 4)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void testSetWithTransactionCanceledException(final Optional<List<String>> maybeCodes, final Class<? extends Exception> expectedException) throws InvalidInputException {
    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
    final ProfilesV2 profilesV2 = new ProfilesV2(dynamoDbClient, mock(DynamoDbAsyncClient.class), Tables.PROFILES_V2.tableName());

    final UUID uuid = UUID.randomUUID();
    final byte[] version = TestRandomUtil.nextBytes(32);
    final byte[] data = TestRandomUtil.nextBytes(256);
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(uuid)).serialize();
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] expectedHash = TestRandomUtil.nextBytes(32);

    final TransactionCanceledException.Builder exceptionBuilder = TransactionCanceledException.builder();
    maybeCodes.ifPresent(codes -> {
      final CancellationReason[] cancellationReasons = codes.stream().map(code ->
          CancellationReason.builder().code(code).build())
          .toArray(CancellationReason[]::new);

      exceptionBuilder.cancellationReasons(cancellationReasons);
    });
    final TransactionCanceledException exception = exceptionBuilder.build();

    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(exception);

    assertThrows(expectedException, () ->
        profilesV2.set(uuid, version, data, VersionedProfile.hash(data), commitment, paymentAddress, VersionedProfile.hash(paymentAddress), expectedHash, getAdditionalTransactWriteItem()));
  }

  static Collection<Arguments> testSetWithTransactionCanceledException() {
    return List.of(
        Arguments.argumentSet("ConditionalCheckFailed throws WriteConflictException", Optional.of(List.of("ConditionalCheckFailed")), WriteConflictException.class),
        Arguments.argumentSet("TransactionConflict throws WriteConflictException", Optional.of(List.of("TransactionConflict")), WriteConflictException.class),
        Arguments.argumentSet("ConditionalCheckFailed throws WriteConflictException", Optional.of(List.of("ConditionalCheckFailed", "ThrottlingError")), WriteConflictException.class),
        Arguments.argumentSet("ConditionalCheckFailed throws WriteConflictException", Optional.of(List.of("ThrottlingError", "ConditionalCheckFailed")), WriteConflictException.class),
        Arguments.argumentSet("ThrottlingError throws TransactionCanceledException", Optional.of(List.of("ThrottlingError")), TransactionCanceledException.class),
        Arguments.argumentSet("No code throws TransactionCanceledException", Optional.empty(), TransactionCanceledException.class)
    );
  }

}
