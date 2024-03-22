/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.assignment;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


public class FullAutoInstancePartitionsUtils extends InstancePartitionsUtils {

  @Override
  public InstancePartitions computeDefaultInstancePartitions(HelixManager helixManager, TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    // Use the corresponding method to compute default instance partitions or no-op.
    return super.computeDefaultInstancePartitions(helixManager, tableConfig, instancePartitionsType);
  }

  @Override
  public InstancePartitions computeDefaultInstancePartitionsForTag(HelixManager helixManager, String tableNameWithType,
      String instancePartitionsType, String serverTag) {
    // Use the corresponding method to compute default instance partitions for tags or no-op.
    return super.computeDefaultInstancePartitionsForTag(helixManager, tableNameWithType, instancePartitionsType,
        serverTag);
  }

  @Override
  public void persistInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore, ConfigAccessor configAccessor,
      String helixClusterName, InstancePartitions instancePartitions) {
    // Use the corresponding method to persist instance partitions.
    super.persistInstancePartitions(propertyStore, configAccessor, helixClusterName, instancePartitions);
  }
}
