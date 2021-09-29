/*
 * Copyright 2020 Restream
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

package ru.rt.restream.reindexer.binding.cproto;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link DataSourceFactory} strategies.
 */
public enum DataSourceFactoryStrategy implements DataSourceFactory {

    /**
     * Next strategy. Selects the next URL cyclically.
     */
    NEXT {
        private int position;

        @Override
        public DataSource getDataSource(List<URI> uris) {
            URI uri = uris.get(position++);
            if (position == uris.size()) {
                position = 0;
            }
            return new PhysicalDataSource(uri);
        }
    },

    /**
     * Random strategy. Select the random URL.
     */
    RANDOM {
        @Override
        public DataSource getDataSource(List<URI> uris) {
            int index = ThreadLocalRandom.current().nextInt(uris.size());
            return new PhysicalDataSource(uris.get(index));
        }
    }

}
