/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.dictionary;

import java.nio.file.Path;

public enum DictionaryType {
    /**
     * Main dictionary of a codec/service. In case of a multi-level protocol it's a
     * dictionary for the top level. This dictionary type is mandatory except some
     * bizarre cases when incoming and outgoing protocols are different
     */
    MAIN,
    /**
     * Dictionary for the lowest level (e.g. a wrapper) in a multi-level protocol
     */
    LEVEL1,
    /**
     * Dictionary for the second level (if there are more than 2 levels) in a multi-level protocol
     */
    LEVEL2,
    /**
     * Dictionary for incoming protocol (used in some very rare cases when incoming and outgoing
     * protocols are different)
     */
    INCOMING,
    /**
     * Dictionary for outgoing protocol (used in some very rare cases when incoming and outgoing
     * protocols are different
     */
    OUTGOING;

    /**
     * @param parent parent directory
     * @return Path to a directory which contains dictionaries with this type
     */
    public Path getDictionary(Path parent) {
        return parent.resolve(name().toLowerCase());
    }
}
