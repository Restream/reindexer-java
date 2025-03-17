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
package ru.rt.restream.reindexer.vector.params;

/**
 * Factories for KnnSearchParams.
 */
public class KnnParams {
    public static BaseKnnSearchParam base(int k) {
        checkK(k);
        return new BaseKnnSearchParam(k);
    }

    public static IndexHnswSearchParam hnsw(int k, int ef) {
        checkK(k);
        if (ef < k) {
            throw new IllegalArgumentException("Minimal value of 'ef' must be greater than or equal to 'k'");
        }
        return new IndexHnswSearchParam(k, ef);
    }

    public static IndexBfSearchParam bf(int k) {
        checkK(k);
        return new IndexBfSearchParam(k);
    }

    public static IndexIvfSearchParam ivf(int k, int nProbe) {
        checkK(k);
        if (nProbe <= 0) {
            throw new IllegalArgumentException("Minimal value of 'nProbe' must be greater than 0");
        }
        return new IndexIvfSearchParam(k, nProbe);
    }

    private static void checkK(int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("'k' must be greater than 0");
        }
    }

}
