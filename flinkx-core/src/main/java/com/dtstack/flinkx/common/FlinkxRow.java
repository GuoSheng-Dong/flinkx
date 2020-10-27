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

package com.dtstack.flinkx.common;

import org.apache.flink.types.Row;

public class FlinkxRow extends Row {
    private int indexOfSubTask;

    public FlinkxRow(final int arity) {
        super(arity);
        this.indexOfSubTask = 0;
    }

    public FlinkxRow(final FlinkxRow row) {
        super(row.getArity());
        this.indexOfSubTask = 0;
        for (int i = 0; i < row.getArity(); ++i) {
            this.setField(i, row.getField(i));
        }
    }

    public static FlinkxRow of(final Object... values) {
        final FlinkxRow row = new FlinkxRow(values.length);
        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }
        return row;
    }

    public void setIndexOfSubTask(final int indexOfSubTask) {
        this.indexOfSubTask = indexOfSubTask;
    }

    public long getObjectSize() {
        long objSize = this.getArity() * 4;
        for (int i = 0; i < this.getArity(); ++i) {
            final Object field = this.getField(i);
            if (null != field) {
                if (field instanceof Integer) {
                    objSize += 4L;
                } else if (field instanceof Long) {
                    objSize += 8L;
                } else if (field instanceof Float) {
                    objSize += 4L;
                } else if (field instanceof Double) {
                    objSize += 8L;
                } else if (field instanceof String) {
                    objSize += ((String) field).length();
                } else if (field instanceof Byte) {
                    ++objSize;
                } else if (field instanceof Boolean) {
                    ++objSize;
                } else if (field instanceof Short) {
                    objSize += 2L;
                } else if (field instanceof Character) {
                    objSize += 2L;
                } else {
                    throw new IllegalArgumentException("The given argument is no support type.");
                }
            }
        }
        
        return objSize;
    }
}
