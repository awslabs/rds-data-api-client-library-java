package com.amazon.rdsdata.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;

@AllArgsConstructor
@Builder
public class MappingOptions {
  public static MappingOptions DEFAULT = MappingOptions.builder()
      .useLabelForMapping(false)
      .build();

  @With public final boolean useLabelForMapping;
}
