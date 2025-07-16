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

import lombok.NonNull;

/**
 * Factories for KnnSearchParams.
 */
public class KnnParams {
    @Deprecated
    public static BaseKnnSearchParam base(int k) {
        checkK(k);
        return new BaseKnnSearchParam(k, null);
    }

    public static BaseKnnSearchParam base(int k, float radius) {
        checkK(k);
        return new BaseKnnSearchParam(k, radius);
    }

    public static BaseKnnSearchParam k(int k) {
        checkK(k);
        return new BaseKnnSearchParam(k, null);
    }

    public static BaseKnnSearchParam radius(float radius) {
        return new BaseKnnSearchParam(null, radius);
    }

    public static IndexHnswSearchParam hnsw(int k, int ef) {
        if (ef < k) {
            throw new IllegalArgumentException("Minimal value of 'ef' must be greater than or equal to 'k'");
        }
        return new IndexHnswSearchParam(k(k), ef);
    }

    public static IndexHnswSearchParam hnsw(@NonNull BaseKnnSearchParam base, int ef) {
        if (base.getK() != null && ef < base.getK()) {
            throw new IllegalArgumentException("Minimal value of 'ef' must be greater than or equal to 'k'");
        }
        return new IndexHnswSearchParam(base, ef);
    }

    public static IndexBfSearchParam bf(int k) {
        return new IndexBfSearchParam(k(k));
    }

    public static IndexBfSearchParam bf(@NonNull BaseKnnSearchParam base) {
        return new IndexBfSearchParam(base);
    }

    public static IndexIvfSearchParam ivf(int k, int nProbe) {
        if (nProbe <= 0) {
            throw new IllegalArgumentException("Minimal value of 'nProbe' must be greater than 0");
        }
        return new IndexIvfSearchParam(k(k), nProbe);
    }

    public static IndexIvfSearchParam ivf(@NonNull BaseKnnSearchParam base, int nProbe) {
        if (nProbe <= 0) {
            throw new IllegalArgumentException("Minimal value of 'nProbe' must be greater than 0");
        }
        return new IndexIvfSearchParam(base, nProbe);
    }

    private static void checkK(int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("'k' must be greater than 0");
        }
    }

}
