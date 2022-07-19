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

package org.apache.iceberg.spark.source;

import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class LessSimpleRecord {
  private Integer id;

  private Integer secondId;

  private String data;

  public LessSimpleRecord() {
  }

  public LessSimpleRecord(Integer id, Integer secondId, String data) {
    this.id = id;
    this.secondId = secondId;
    this.data = data;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public Integer getSecondId() {
    return secondId;
  }

  public void setSecondId(Integer secondId) {
    this.secondId = secondId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LessSimpleRecord record = (LessSimpleRecord) o;
    return Objects.equal(id, record.id) && Objects.equal(data, record.data);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, secondId, data);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\"id\"=");
    buffer.append(id);
    buffer.append(",\"secondId\"=\"");
    buffer.append(secondId);
    buffer.append(",\"data\"=\"");
    buffer.append(data);
    buffer.append("\"}");
    return buffer.toString();
  }
}
