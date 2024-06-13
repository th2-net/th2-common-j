/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.box.configuration;

import com.exactpro.th2.common.schema.configuration.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.exactpro.th2.common.event.EventUtils.requireNonBlankBookName;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

public class BoxConfiguration extends Configuration {
    public static final String DEFAULT_BOOK_NAME = "test_book";
    public static final String DEFAULT_BOX_NAME = "th2_component";

    @JsonProperty
    private String boxName = DEFAULT_BOX_NAME;

    @JsonProperty
    private String bookName = DEFAULT_BOOK_NAME;

    @NotNull
    public String getBoxName() {
        return boxName;
    }

    public void setBoxName(@Nullable String boxName) {
        this.boxName = defaultIfBlank(boxName, DEFAULT_BOX_NAME);
    }

    @NotNull
    public String getBookName() {
        return bookName;
    }

    public void setBookName(@NotNull String bookName) {
        this.bookName = requireNonBlankBookName(bookName);
    }
}
