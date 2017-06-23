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
package org.apache.drill.exec.store.rest.read;

import org.apache.commons.io.Charsets;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.rest.RestSubScan;
import org.apache.drill.exec.store.rest.functions.FunctionsHelper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.json.XML;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
public final class XmlRestRecordReader extends JsonRestRecordReader {
    public XmlRestRecordReader(FragmentContext fragmentContext,
                               RestSubScan scan,
                               CloseableHttpResponse response) {
        super(fragmentContext, scan, response);
    }

    @Override
    void setupParser() throws IOException {
        String xml = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
        String result = FunctionsHelper.removeNamespaces(xml);
        JSONObject xmlJSONObj = XML.toJSONObject(result);
        String json = xmlJSONObj.toString();
        jsonReader.setSource(new ByteArrayInputStream(json.getBytes(Charsets.UTF_8)));

    }

}
