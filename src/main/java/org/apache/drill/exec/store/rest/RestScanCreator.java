package org.apache.drill.exec.store.rest;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
public class RestScanCreator implements BatchCreator<RestSubScan> {
    @Override
    public CloseableRecordBatch getBatch(FragmentContext fragmentContext, RestSubScan physicalOperators, List<RecordBatch> list) throws ExecutionSetupException {
        return null;
    }
}
