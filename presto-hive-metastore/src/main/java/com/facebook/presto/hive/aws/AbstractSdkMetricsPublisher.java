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
package com.facebook.presto.hive.aws;

import software.amazon.awssdk.core.internal.metrics.SdkErrorType;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static software.amazon.awssdk.core.metrics.CoreMetric.API_CALL_DURATION;
import static software.amazon.awssdk.core.metrics.CoreMetric.BACKOFF_DELAY_DURATION;
import static software.amazon.awssdk.core.metrics.CoreMetric.ERROR_TYPE;
import static software.amazon.awssdk.core.metrics.CoreMetric.RETRY_COUNT;
import static software.amazon.awssdk.core.metrics.CoreMetric.SERVICE_CALL_DURATION;

public abstract class AbstractSdkMetricsPublisher
        implements MetricPublisher
{
    @Override
    public void publish(MetricCollection metricCollection)
    {
        List<Duration> apiCallDurationList = metricCollection.metricValues(API_CALL_DURATION);
        List<Integer> retryCountList = metricCollection.metricValues(RETRY_COUNT);

        List<Duration> backOffDelayDurationList = new ArrayList<>();
        List<Duration> serviceCalDurationList = new ArrayList<>();

        // These metrics are in the second level
        for (MetricCollection mc : metricCollection.children()) {
            serviceCalDurationList.addAll(mc.metricValues(SERVICE_CALL_DURATION));
            backOffDelayDurationList.addAll(mc.metricValues(BACKOFF_DELAY_DURATION));
        }

        int retryCount = retryCountList.stream().reduce(Integer::sum).orElse(0);
        int requestCount = retryCount + retryCountList.size();
        long throttleExceptions = metricCollection.metricValues(ERROR_TYPE)
                .stream()
                .filter(s -> s.equals(SdkErrorType.THROTTLING.toString()))
                .count();

        recordRetryCount(retryCount);
        recordRequestCount(requestCount);
        recordThrottleExceptionCount(throttleExceptions);
        aggregrateDurations(serviceCalDurationList, this::recordHttpRequestTime);
        aggregrateDurations(backOffDelayDurationList, this::recordRetryPauseTime);
        aggregrateDurations(apiCallDurationList, this::recordClientExecutionTime);
    }

    protected abstract void recordRequestCount(long count);
    protected abstract void recordRetryCount(long count);

    protected abstract void recordHttpRequestTime(io.airlift.units.Duration duration);
    protected abstract void recordClientExecutionTime(io.airlift.units.Duration duration);
    protected abstract void recordRetryPauseTime(io.airlift.units.Duration duration);
    protected abstract void recordThrottleExceptionCount(long count);

    private static void aggregrateDurations(List<Duration> durations, Consumer<io.airlift.units.Duration> consumer)
    {
        if (durations != null) {
            for (Duration duration : durations) {
                if (duration == null) {
                    continue;
                }
                consumer.accept(new io.airlift.units.Duration(duration.toMillis(), TimeUnit.MILLISECONDS));
            }
        }
    }

    @Override
    public void close()
    {
    }
}
