package org.apache.drill.exec.store.rest.read;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.rest.FilterPushDown;
import org.apache.drill.exec.store.rest.RestSubScan;
import org.apache.drill.exec.vector.complex.fn.WorkingBufferProxy;
import org.apache.drill.exec.vector.complex.impl.EnhancedVectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * @author Oleg Zinoviev
 * @since 20.06.2017.
 */
public class TextRestRecordReader extends AbstractRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextRestRecordReader.class);
    private static final String CONTENT_COLUMN = "content";

    private final RestSubScan scan;
    private final CloseableHttpClient client;
    private final CloseableHttpResponse response;
    private final WorkingBufferProxy workingBuffer;
    private final FragmentContext fragmentContext;

    private EnhancedVectorContainerWriter writer;
    private boolean read = false;

    public TextRestRecordReader(FragmentContext fragmentContext,
                                RestSubScan scan,
                                CloseableHttpClient client,
                                CloseableHttpResponse response) {
        this.fragmentContext = fragmentContext;
        this.scan = scan;
        this.client = client;
        this.response = response;
        this.workingBuffer = new WorkingBufferProxy(fragmentContext.getManagedBuffer());
    }


    @Override
    public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
        try{
            this.writer = new EnhancedVectorContainerWriter(
                    fragmentContext.getManagedBuffer(),
                    output,
                    false,
                    scan.getSpec().getFilterPushDown() == FilterPushDown.SOME ? scan.getSpec().getParameters() : Collections.emptyMap());
        }catch(final Exception e){
            handleAndRaise(e);
        }
    }

    @Override
    public int next() {
        writer.allocate();
        writer.reset();
        int recordCount = 0;
        if (read) {
            return recordCount;
        }

        try {
            String text = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (StringUtils.isNotEmpty(text)) {
                writer.setPosition(recordCount);
                BaseWriter.MapWriter mapWriter = writer.rootAsMap();
                mapWriter.start();

                mapWriter.varChar(CONTENT_COLUMN).writeVarChar(0,
                        workingBuffer.prepareVarCharHolder(text),
                        workingBuffer.getBuf());

                mapWriter.end();

                recordCount++;
                writer.setValueCount(recordCount);
            }
        } catch (Exception e) {
            handleAndRaise(e);
        } finally {
            read = true;
        }

        return recordCount;
    }

    @Override
    public void close() throws Exception {
        response.close();
        client.close();
        writer.close();
    }

    private void handleAndRaise(Exception e) throws UserException {
        String message = e.getMessage();

        UserException.Builder exceptionBuilder = UserException.dataReadError(e)
                .message("%s - %s", "Failure reading TEXT", message);

        throw exceptionBuilder.build(logger);
    }
}
