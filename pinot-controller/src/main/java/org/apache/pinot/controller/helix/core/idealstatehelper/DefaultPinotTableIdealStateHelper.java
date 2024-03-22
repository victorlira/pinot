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
package org.apache.pinot.controller.helix.core.idealstatehelper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionGroupMetadataFetcher;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultPinotTableIdealStateHelper implements PinotTableIdealStateHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPinotTableIdealStateHelper.class);
  private static final RetryPolicy DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY =
      RetryPolicies.randomDelayRetryPolicy(3, 100L, 200L);

  @Override
  public IdealState buildEmptyIdealStateFor(TableConfig tableConfig, boolean enableBatchMessageMode) {
    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getReplication();
    LOGGER.info("Building CUSTOM IdealState for Table: {}, numReplicas: {}", tableNameWithType, numReplicas);
    CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableNameWithType);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(numReplicas).setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableNameWithType);
    idealState.setBatchMessageMode(enableBatchMessageMode);
    return idealState;
  }

  @Override
  public void assignSegment(HelixManager helixManager, String tableNameWithType, String segmentName,
      SegmentAssignment segmentAssignment, Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    HelixHelper.updateIdealState(helixManager, tableNameWithType, idealState -> {
      assert idealState != null;
      Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
      if (currentAssignment.containsKey(segmentName)) {
        LOGGER.warn("Segment: {} already exists in the IdealState for table: {}, do not update", segmentName,
            tableNameWithType);
      } else {
        List<String> assignedInstances =
            segmentAssignment.assignSegment(segmentName, currentAssignment, instancePartitionsMap);
        LOGGER.info("Assigning segment: {} to instances: {} for table: {}", segmentName, assignedInstances,
            tableNameWithType);
        currentAssignment.put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(assignedInstances,
            CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
      }
      return idealState;
    });
  }

  @Override
  public void updateInstanceStatesForNewConsumingSegment(IdealState idealState, @Nullable String committingSegmentName,
      @Nullable String newSegmentName, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();
    if (committingSegmentName != null) {
      // Change committing segment state to ONLINE
      Set<String> instances = instanceStatesMap.get(committingSegmentName).keySet();
      instanceStatesMap.put(committingSegmentName, SegmentAssignmentUtils.getInstanceStateMap(instances,
          CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
      LOGGER.info("Updating segment: {} to ONLINE state", committingSegmentName);
    }

    // There used to be a race condition in pinot (caused by heavy GC on the controller during segment commit)
    // that ended up creating multiple consuming segments for the same stream partition, named somewhat like
    // tableName__1__25__20210920T190005Z and tableName__1__25__20210920T190007Z. It was fixed by checking the
    // Zookeeper Stat object before updating the segment metadata.
    // These conditions can happen again due to manual operations considered as fixes in Issues #5559 and #5263
    // The following check prevents the table from going into such a state (but does not prevent the root cause
    // of attempting such a zk update).
    if (newSegmentName != null) {
      LLCSegmentName newLLCSegmentName = new LLCSegmentName(newSegmentName);
      int partitionId = newLLCSegmentName.getPartitionGroupId();
      int seqNum = newLLCSegmentName.getSequenceNumber();
      for (String segmentNameStr : instanceStatesMap.keySet()) {
        LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentNameStr);
        if (llcSegmentName == null) {
          // skip the segment name if the name is not in low-level consumer format
          // such segment name can appear for uploaded segment
          LOGGER.debug("Skip segment name {} not in low-level consumer format", segmentNameStr);
          continue;
        }
        if (llcSegmentName.getPartitionGroupId() == partitionId && llcSegmentName.getSequenceNumber() == seqNum) {
          String errorMsg =
              String.format("Segment %s is a duplicate of existing segment %s", newSegmentName, segmentNameStr);
          LOGGER.error(errorMsg);
          throw new HelixHelper.PermanentUpdaterException(errorMsg);
        }
      }
      // Assign instances to the new segment and add instances as state CONSUMING
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(newSegmentName, instanceStatesMap, instancePartitionsMap);
      instanceStatesMap.put(newSegmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned,
          CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING));
      LOGGER.info("Adding new CONSUMING segment: {} to instances: {}", newSegmentName, instancesAssigned);
    }
  }

  @Override
  public List<PartitionGroupMetadata> getPartitionGroupMetadataList(StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList) {
    PartitionGroupMetadataFetcher partitionGroupMetadataFetcher =
        new PartitionGroupMetadataFetcher(streamConfig, partitionGroupConsumptionStatusList);
    try {
      DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY.attempt(partitionGroupMetadataFetcher);
      return partitionGroupMetadataFetcher.getPartitionGroupMetadataList();
    } catch (Exception e) {
      Exception fetcherException = partitionGroupMetadataFetcher.getException();
      LOGGER.error("Could not get PartitionGroupMetadata for topic: {} of table: {}", streamConfig.getTopicName(),
          streamConfig.getTableNameWithType(), fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }
}
