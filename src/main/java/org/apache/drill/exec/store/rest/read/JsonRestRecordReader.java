package org.apache.drill.exec.store.rest.read;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.CountingJsonReader;
import org.apache.drill.exec.store.rest.FilterPushDown;
import org.apache.drill.exec.store.rest.RestSubScan;
import org.apache.drill.exec.vector.complex.fn.RestJsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.utils.HttpClientUtils;

import java.io.IOException;
import java.util.Collections;

import static org.apache.drill.exec.store.easy.json.JSONRecordReader.DEFAULT_ROWS_PER_BATCH;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
public final class JsonRestRecordReader extends AbstractRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonRestRecordReader.class);

    private final FragmentContext fragmentContext;
    private final RestSubScan scan;
    private final CloseableHttpResponse response;
    private final boolean enableAllTextMode;
    private final boolean readNumbersAsDouble;
    private final boolean unionEnabled;

    private JsonProcessor jsonReader;
    private VectorContainerWriter writer;
    private JsonProcessor.ReadState write = null;

    public JsonRestRecordReader(FragmentContext fragmentContext,
                                RestSubScan scan,
                                CloseableHttpResponse response) {
        this.fragmentContext = fragmentContext;
        this.scan = scan;
        this.response = response;

        this.enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
        this.readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);
        this.unionEnabled = fragmentContext.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
    }


    @Override
    public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
        try{
            this.writer = new VectorContainerWriter(output, unionEnabled);
            if (isSkipQuery()) {
                this.jsonReader = new CountingJsonReader(fragmentContext.getManagedBuffer());
            } else {
                this.jsonReader = new RestJsonReader(fragmentContext.getManagedBuffer(),
                        enableAllTextMode,
                        true,
                        readNumbersAsDouble,
                        scan.getSpec().getFilterPushDown() == FilterPushDown.SOME ? scan.getSpec().getParameters() : Collections.emptyMap());
            }
            setupParser();
        }catch(final Exception e){
            handleAndRaise(e);
        }
    }

    private void setupParser() throws IOException {
        jsonReader.setSource(response.getEntity().getContent());
    }

    private void handleAndRaise(Exception e) throws UserException {

        String message = e.getMessage();
        int columnNr = -1;

        if (e instanceof JsonParseException) {
            final JsonParseException ex = (JsonParseException) e;
            message = ex.getOriginalMessage();
            columnNr = ex.getLocation().getColumnNr();
        }

        UserException.Builder exceptionBuilder = UserException.dataReadError(e)
                .message("%s - %s", "Failure reading JSON", message);
        if (columnNr > 0) {
            exceptionBuilder.pushContext("Column ", columnNr);
        }

        throw exceptionBuilder.build(logger);
    }

    @Override
    public int next() {
        writer.allocate();
        writer.reset();
        int recordCount = 0;
        if(write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR){
            return recordCount;
        }

        while (recordCount < DEFAULT_ROWS_PER_BATCH) {
            try {
                writer.setPosition(recordCount);
                write = jsonReader.write(writer);
                if (write == JsonProcessor.ReadState.WRITE_SUCCEED) {
                    recordCount++;
                } else if (write == JsonProcessor.ReadState.JSON_RECORD_PARSE_ERROR || write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR) {
                    handleAndRaise(new Exception(scan.getSpec().getQuery() + " : line nos :" + (recordCount + 1)));
                } else {
                    break;
                }
            } catch (IOException ex) {
                handleAndRaise(ex);
            }
        }
        jsonReader.ensureAtLeastOneField(writer);
        writer.setValueCount(recordCount);
        return recordCount;
    }

    @Override
    public void close() throws Exception {
        HttpClientUtils.closeQuietly(response);
        writer.close();
    }
}
