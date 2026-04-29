/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;
import java.util.Arrays;
import java.util.Objects;

public record VersionedProfileV1(String version,
                                 @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                 @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                 byte[] name,

                                 @Nullable
                                 String avatar,

                                 @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                 @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                 byte[] aboutEmoji,

                                 @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                 @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                 byte[] about,

                                 @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                 @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                 byte[] paymentAddress,

                                 @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                 @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                 byte[] phoneNumberSharing,

                                 @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                 @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                 byte[] commitment) {

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof final VersionedProfileV1 that))
      return false;

    return Arrays.equals(name, that.name) && Arrays.equals(about, that.about) && Objects.equals(avatar, that.avatar)
        && Objects.equals(version, that.version) && Arrays.equals(aboutEmoji, that.aboutEmoji) && Arrays.equals(
        commitment, that.commitment) && Arrays.equals(paymentAddress, that.paymentAddress) && Arrays.equals(
        phoneNumberSharing, that.phoneNumberSharing);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        version,
        Arrays.hashCode(name),
        Objects.hashCode(avatar),
        Arrays.hashCode(aboutEmoji),
        Arrays.hashCode(about),
        Arrays.hashCode(paymentAddress),
        Arrays.hashCode(phoneNumberSharing),
        Arrays.hashCode(commitment));
  }
}
