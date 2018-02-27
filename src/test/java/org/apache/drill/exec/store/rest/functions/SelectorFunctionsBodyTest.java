/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.rest.functions;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Oleg Zinoviev
 * @since 27.02.18.
 */
@SuppressWarnings("deprecation")
public class SelectorFunctionsBodyTest {
    @Test
    public void testJPathNull() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "jpath";
        ObjectHolder source = new ObjectHolder();
        source.obj = null;
        ObjectHolder path = new ObjectHolder();
        path.obj = null;

        SelectorFunctionsBody.select(type, source, path);
    }

    @Test
    public void testXPathNull() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "xpath";
        ObjectHolder source = new ObjectHolder();
        source.obj = null;
        ObjectHolder path = new ObjectHolder();
        path.obj = null;

        SelectorFunctionsBody.select(type, source, path);
    }

    @Test
    public void testCssNull() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "css";
        ObjectHolder source = new ObjectHolder();
        source.obj = null;
        ObjectHolder path = new ObjectHolder();
        path.obj = null;

        SelectorFunctionsBody.select(type, source, path);
    }

    @Test(expected = UserException.class)
    public void testJPathNullSelector() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "jpath";
        ObjectHolder source = new ObjectHolder();
        source.obj = "{}";
        ObjectHolder path = new ObjectHolder();
        path.obj = null;

        SelectorFunctionsBody.select(type, source, path);
        Assert.fail();
    }

    @Test(expected = UserException.class)
    public void testXPathNullSelector() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "xpath";
        ObjectHolder source = new ObjectHolder();
        source.obj = "<r />";
        ObjectHolder path = new ObjectHolder();
        path.obj = null;

        SelectorFunctionsBody.select(type, source, path);
        Assert.fail();
    }

    @Test(expected = UserException.class)
    public void testCssNullSelector() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "css";
        ObjectHolder source = new ObjectHolder();
        source.obj = "<html> </html>";
        ObjectHolder path = new ObjectHolder();
        path.obj = null;

        SelectorFunctionsBody.select(type, source, path);
        Assert.fail();
    }

    @Test
    public void testJPathEmptyResult() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "jpath";
        ObjectHolder source = new ObjectHolder();
        source.obj = "{}";
        ObjectHolder path = new ObjectHolder();
        path.obj = "$.data[*]";

        Iterable<byte[]> select = SelectorFunctionsBody.select(type, source, path);
        Assert.assertNotNull(select);
        Assert.assertTrue(!select.iterator().hasNext());
    }

    @Test
    public void testXPathEmptyResult() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "xpath";
        ObjectHolder source = new ObjectHolder();
        source.obj = "<r />";
        ObjectHolder path = new ObjectHolder();
        path.obj = "//data";

        Iterable<byte[]> select = SelectorFunctionsBody.select(type, source, path);
        Assert.assertNotNull(select);
        Assert.assertTrue(!select.iterator().hasNext());
    }

    @Test
    public void testCssEmptyResult() {
        ObjectHolder type = new ObjectHolder();
        type.obj = "css";
        ObjectHolder source = new ObjectHolder();
        source.obj = "<html> </html>";
        ObjectHolder path = new ObjectHolder();
        path.obj = ".a";

        Iterable<byte[]> select = SelectorFunctionsBody.select(type, source, path);
        Assert.assertNotNull(select);
        Assert.assertTrue(!select.iterator().hasNext());
    }
}
