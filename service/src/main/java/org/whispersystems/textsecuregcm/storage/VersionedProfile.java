/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record VersionedProfile(@JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                               @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                               byte[] version,

                               @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                               @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                               byte[] data,

                               @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                               @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                               byte[] dataHash,

                               @Nullable
                               @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                               @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                               byte[] paymentAddress,

                               @Nullable
                               @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                               @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                               byte[] paymentAddressHash,

                               @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                               @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                               byte[] commitment) {

  public VersionedProfile(byte[] version, byte[] data, @Nullable byte[] paymentAddress, byte[] commitment) {
    this(Objects.requireNonNull(version), Objects.requireNonNull(data), hash(Objects.requireNonNull(data)), paymentAddress, paymentAddress == null ? null : hash(paymentAddress), Objects.requireNonNull(commitment));
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof final VersionedProfile that))
      return false;

    return Arrays.equals(data, that.data) && Arrays.equals(version, that.version) && Arrays.equals(dataHash,
        that.dataHash) && Arrays.equals(commitment, that.commitment) && Arrays.equals(paymentAddress,
        that.paymentAddress) && Arrays.equals(paymentAddressHash, that.paymentAddressHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        Arrays.hashCode(version),
        Arrays.hashCode(data),
        Arrays.hashCode(dataHash),
        Arrays.hashCode(paymentAddress),
        Arrays.hashCode(paymentAddressHash),
        Arrays.hashCode(commitment));
  }

  /**
   * Computes the SHA-256 hash of the given data.
   */
  @VisibleForTesting
  static byte[] hash(final byte[] data) {
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    return sha256.digest(data);
  }

}
