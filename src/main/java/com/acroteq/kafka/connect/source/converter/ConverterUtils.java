package com.acroteq.kafka.connect.source.converter;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Optional;
import lombok.NonNull;
import org.jetbrains.annotations.Nullable;

final class ConverterUtils {

    private ConverterUtils() {
    }

    @Nullable
    static Date convertOptionalDate(@Nullable final OffsetDateTime date) {
        return Optional.ofNullable(date)
                       .map(offset -> Date.from(offset.toInstant()))
                       .orElse(null);
    }

    @NonNull
    static Date convertDate(@NonNull final OffsetDateTime date) {
        return Date.from(date.toInstant());
    }
}
