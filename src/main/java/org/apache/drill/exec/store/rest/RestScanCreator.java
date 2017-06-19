package org.apache.drill.exec.store.rest;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.io.IOException;
import java.net.URISyntaxException;
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
            return scan.getStoragePlugin().createBatchScan(context, scan);
        } catch (URISyntaxException | IOException e) {
            throw new ExecutionSetupException(e);
        }
    }
}
