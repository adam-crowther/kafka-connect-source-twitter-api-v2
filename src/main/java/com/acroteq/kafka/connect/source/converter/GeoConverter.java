/*
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * For more information, please refer to <http://unlicense.org/>
 */
package com.acroteq.kafka.connect.source.converter;

import static com.twitter.clientlib.model.Point.SERIALIZED_NAME_TYPE;
import static com.twitter.clientlib.model.TweetGeo.SERIALIZED_NAME_PLACE_ID;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Decimal.schema;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

import com.twitter.clientlib.model.Point;
import com.twitter.clientlib.model.TweetGeo;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
class GeoConverter {

    private static final int POINTS_COORDINATES_SCALE = 8;

    private static final Schema POINT_SCHEMA = struct().optional()
                                                       .field(SERIALIZED_NAME_TYPE, STRING_SCHEMA)
                                                       .field(Point.SERIALIZED_NAME_COORDINATES,
                                                              array(schema(POINTS_COORDINATES_SCALE)))
                                                       .build();
    static final Schema GEO_SCHEMA = struct().optional()
                                             .field(TweetGeo.SERIALIZED_NAME_COORDINATES, POINT_SCHEMA)
                                             .field(SERIALIZED_NAME_PLACE_ID, OPTIONAL_STRING_SCHEMA)
                                             .build();

    private Struct convert(@NonNull final Point input) {
        return new Struct(POINT_SCHEMA).put(SERIALIZED_NAME_TYPE,
                                            input.getType()
                                                 .getValue())
                                       .put(Point.SERIALIZED_NAME_COORDINATES,
                                            input.getCoordinates()
                                                 .stream()
                                                 .map(d -> d.setScale(POINTS_COORDINATES_SCALE, UNNECESSARY))
                                                 .collect(toList()));
    }

    private Struct convertOptional(@Nullable final Point input) {
        return Optional.ofNullable(input)
                       .map(this::convert)
                       .orElse(null);
    }

    private Struct convert(@NonNull final TweetGeo input) {
        return new Struct(GEO_SCHEMA).put(TweetGeo.SERIALIZED_NAME_COORDINATES, convertOptional(input.getCoordinates()))
                                     .put(SERIALIZED_NAME_PLACE_ID, input.getPlaceId());
    }

    @org.jetbrains.annotations.Nullable
    Struct convertOptional(final TweetGeo geo) {
        return Optional.ofNullable(geo)
                       .map(this::convert)
                       .orElse(null);
    }
}
