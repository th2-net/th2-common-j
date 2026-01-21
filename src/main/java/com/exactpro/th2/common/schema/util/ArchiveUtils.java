/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.common.schema.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class ArchiveUtils {

    private static final GzipBase64StringEncoder GZIP_BASE_64_STRING_ENCODER = new GzipBase64StringEncoder();

    private static final GzipBase64StringDecoder GZIP_BASE_64_STRING_DECODER = new GzipBase64StringDecoder();


    private ArchiveUtils() {
        throw new AssertionError();
    }


    public static GzipBase64StringEncoder getGzipBase64StringEncoder() {
        return GZIP_BASE_64_STRING_ENCODER;
    }

    public static GzipBase64StringDecoder getGzipBase64StringDecoder() {
        return GZIP_BASE_64_STRING_DECODER;
    }


    public static class GzipBase64StringEncoder {

        private GzipBase64StringEncoder() {
        }

        public byte[] encode(String value) throws IOException {

            var baos = new ByteArrayOutputStream();

            var gziposs = new GZIPOutputStream(baos);

            try (baos; gziposs) {
                gziposs.write(value.getBytes());
                gziposs.finish();
                return Base64.getEncoder().encode(baos.toByteArray());
            }

        }

    }

    public static class GzipBase64StringDecoder {

        private GzipBase64StringDecoder() {
        }

        public byte[] decode(String value) throws IOException {

            var decodedValue = Base64.getDecoder().decode(value.strip());

            var bais = new ByteArrayInputStream(decodedValue);

            var gzipis = new GZIPInputStream(bais);

            var buffGzis = new BufferedInputStream(gzipis);

            try (bais; gzipis; buffGzis) {
                return buffGzis.readAllBytes();
            }

        }

    }

}
