/*
 * Copyright 2020-present Restream
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

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.exceptions.ReindexerExceptionFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * A {@link DataSource} that wraps a target {@link DataSource} and instruments it using configured {@link ObservationRegistry}.
 */
@RequiredArgsConstructor
final class ObservationDataSource implements DataSource {

    private static final CommandObservationConvention CONVENTION = new CommandObservationConvention();

    private final DataSource delegate;

    private final String url;

    private final ObservationRegistry registry;

    @Override
    public Connection getConnection(Duration timeout, ScheduledThreadPoolExecutor scheduler) {
        Connection connection = delegate.getConnection(timeout, scheduler);
        return new ObservationConnection(connection);
    }

    @RequiredArgsConstructor
    private final class ObservationConnection implements Connection {

        private final Connection delegate;

        @Override
        public ReindexerResponse rpcCall(int command, Object... args) {
            CommandObservationContext context = new CommandObservationContext(command, args);
            context.setRemoteServiceAddress(url);
            Observation observation = Observation.createNotStarted(CONVENTION, () -> context, registry);
            return observation.observe(() -> {
                ReindexerResponse response = delegate.rpcCall(command, args);
                context.setResponse(response);
                if (response.hasError()) {
                    observation.error(ReindexerExceptionFactory.fromResponse(response));
                }
                return response;
            });
        }

        @Override
        public CompletableFuture<ReindexerResponse> rpcCallAsync(int command, Object... args) {
            CommandObservationContext context = new CommandObservationContext(command, args);
            context.setRemoteServiceAddress(url);
            Observation observation = Observation.createNotStarted(CONVENTION, () -> context, registry).start();
            CompletableFuture<ReindexerResponse> future;
            try (Observation.Scope scope = observation.openScope()) {
                future = delegate.rpcCallAsync(command, args);
            } catch (Throwable t) {
                observation.error(t);
                observation.stop();
                throw t;
            }
            return future.whenComplete((response, error) -> {
                if (error != null) {
                    observation.error(error);
                } else {
                    context.setResponse(response);
                    if (response.hasError()) {
                        observation.error(ReindexerExceptionFactory.fromResponse(response));
                    }
                }
                observation.stop();
            });
        }

        @Override
        public boolean hasError() {
            return delegate.hasError();
        }

        @Override
        public void close() {
            delegate.close();
        }

    }

}
