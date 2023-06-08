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

import com.facebook.presto.spi.PrestoException;
import software.amazon.awssdk.services.glue.model.GlueException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;

public class AwsSdkUtil
{
    private AwsSdkUtil()
    {
    }
    /**
     * Helper method to handle sync request with async client
     */
    public static <R1, R2> R2 awsSyncRequest(
            Function<R1, CompletableFuture<R2>> submission,
            R1 request,
            GlueCatalogApiStats stats) throws GlueException
    {
        requireNonNull(submission, "submission is null");
        requireNonNull(request, "request is null");
        try {
            if (stats != null) {
                return stats.record(() -> submission.apply(request).join());
            }

            return submission.apply(request).join();
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof GlueException) {
                throw (GlueException) e.getCause();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }
}
