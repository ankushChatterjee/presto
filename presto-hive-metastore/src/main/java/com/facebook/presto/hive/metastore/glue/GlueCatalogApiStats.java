/*
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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class GlueCatalogApiStats
{
    private final TimeStat time = new TimeStat(MILLISECONDS);
    private final CounterStat totalFailures = new CounterStat();

    class MetricsAsyncHandler
    {
        private GlueCatalogApiStats stats;
        private final TimeStat.BlockTimer timer;
        public MetricsAsyncHandler(GlueCatalogApiStats stats)
        {
            this.stats = stats;
            this.timer = stats.time.time();
        }

        public void onError(Throwable exception)
        {
            timer.close();
            stats.recordException(exception);
        }

        public void onSuccess()
        {
            timer.close();
        }
    }

    public <T> T record(Supplier<T> action)
    {
        try (TimeStat.BlockTimer timer = time.time()) {
            return action.get();
        }
        catch (Exception e) {
            recordException(e);
            throw e;
        }
    }

    public void record(Runnable action)
    {
        try (TimeStat.BlockTimer timer = time.time()) {
            action.run();
        }
        catch (Exception e) {
            recordException(e);
            throw e;
        }
    }

    public MetricsAsyncHandler metricsAsyncHandler()
    {
        return new MetricsAsyncHandler(this);
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getTotalFailures()
    {
        return totalFailures;
    }

    private void recordException(Throwable e)
    {
        totalFailures.update(1);
    }
}
