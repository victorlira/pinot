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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixOfflineSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FullAutoPinotTableIdealStateHelper extends DefaultPinotTableIdealStateHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FullAutoPinotTableIdealStateHelper.class);

  @Override
  public IdealState buildEmptyIdealStateFor(TableConfig tableConfig, boolean enableBatchMessageMode) {
    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getReplication();

    LOGGER.info("Building FULL-AUTO IdealState for Table: {}, numReplicas: {}", tableNameWithType, numReplicas);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    String stateModel;
    if (tableType == null) {
      throw new RuntimeException("Failed to get table type from table name: " + tableNameWithType);
    } else if (TableType.OFFLINE.equals(tableType)) {
      stateModel =
          PinotHelixOfflineSegmentOnlineOfflineStateModelGenerator.PINOT_OFFLINE_SEGMENT_ONLINE_OFFLINE_STATE_MODEL;
    } else {
      stateModel =
          PinotHelixOfflineSegmentOnlineOfflineStateModelGenerator.PINOT_OFFLINE_SEGMENT_ONLINE_OFFLINE_STATE_MODEL;
    }

    // FULL-AUTO Segment Online-Offline state model with a rebalance strategy, crushed auto-rebalance by default
    // TODO: The state model used only works for OFFLINE tables today. Add support for REALTIME state model too
    FullAutoModeISBuilder idealStateBuilder = new FullAutoModeISBuilder(tableNameWithType);
    idealStateBuilder
        .setStateModel(stateModel)
        .setNumPartitions(0).setNumReplica(numReplicas).setMaxPartitionsPerNode(1)
        // TODO: Revisit the rebalance strategy to use (maybe we add a custom one)
        .setRebalanceStrategy(CrushEdRebalanceStrategy.class.getName());
    // The below config guarantees if active number of replicas is no less than minimum active replica, there will
    // not be partition movements happened.
    // Set min active replicas to 0 and rebalance delay to 5 minutes so that if any master goes offline, Helix
    // controller waits at most 5 minutes and then re-calculate the participant assignment.
    // TODO: Assess which of these values need to be tweaked, removed, and what additional values that need to be added
    idealStateBuilder.setMinActiveReplica(numReplicas - 1);
    idealStateBuilder.setRebalanceDelay(300_000);
    idealStateBuilder.enableDelayRebalance();
    // Set instance group tag
    IdealState idealState = idealStateBuilder.build();
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
      Map<String, List<String>> currentAssignmentList = idealState.getRecord().getListFields();
      if (currentAssignment.containsKey(segmentName) && currentAssignmentList.containsKey(segmentName)) {
        LOGGER.warn("Segment: {} already exists in the IdealState for table: {}, do not update", segmentName,
            tableNameWithType);
      } else {
        List<String> assignedInstances =
            segmentAssignment.assignSegment(segmentName, currentAssignment, instancePartitionsMap);
        LOGGER.info("Assigning segment: {} to instances: {} for table: {}", segmentName, assignedInstances,
            tableNameWithType);
        currentAssignmentList.put(segmentName, Collections.emptyList());
      }
      return idealState;
    });
  }

  @Override
  public void updateInstanceStatesForNewConsumingSegment(IdealState idealState, @Nullable String committingSegmentName,
      @Nullable String newSegmentName, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();
    Map<String, List<String>> segmentList = idealState.getRecord().getListFields();
    // TODO: Need to figure out the best way to handle committed segments' state change
    if (committingSegmentName != null) {
      // Change committing segment state to ONLINE
//      Set<String> instances = instanceStatesMap.get(committingSegmentName).keySet();
//      instanceStatesMap.put(committingSegmentName,
//          SegmentAssignmentUtils.getInstanceStateMap(instances, SegmentStateModel.ONLINE));
//      instanceStatesList.put(newSegmentName, Collections.emptyList()
//          /*SegmentAssignmentUtils.getInstanceStateList(instancesAssigned)*/);
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
      // No need to check for tableType as offline tables can never go to CONSUMING state. All callers are for REALTIME
//      instanceStatesMap.put(newSegmentName,
//          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.CONSUMING));
      // TODO: Once REALTIME segments move to FULL-AUTO, we cannot update the map. Uncomment below lines to update list.
      //       Assess whether we should set am empty InstanceStateList for the segment or not. i.e. do we support
      //       this preferred list concept, and does Helix-Auto even allow preferred list concept (from code reading it
      //       looks like it does)
      segmentList.put(newSegmentName, Collections.emptyList()
          /*SegmentAssignmentUtils.getInstanceStateList(instancesAssigned)*/);
      LOGGER.info("Adding new CONSUMING segment: {} to instances: {}", newSegmentName, instancesAssigned);
    }
  }
}
