/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.rest;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.read.RestRecordReader;

import java.util.Collections;
import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
@SuppressWarnings("unused")
public class RestScanCreator implements BatchCreator<RestSubScan> {

    @Override
    public CloseableRecordBatch getBatch(FragmentContext context, RestSubScan scan, List<RecordBatch> children) throws ExecutionSetupException {
        assert children == null || children.isEmpty();
        try {
            return createBatchScan(context, scan);
        } catch (RuntimeException e) {
            throw new ExecutionSetupException(e);
        }
    }

    private CloseableRecordBatch createBatchScan(FragmentContext context, RestSubScan scan) {

        OperatorContext operatorContext = context.newOperatorContext(scan);

        RuntimeQueryConfig config = scan.getStoragePlugin().getConfig().getRuntimeConfig(scan.getSpec().getQuery());

        RecordReader reader = new RestRecordReader(context, scan, new RequestHandler(config));

        return new ScanBatch(scan, context, operatorContext, Collections.singletonList(reader), Collections.emptyList());
    }


}
