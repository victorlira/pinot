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
package org.apache.pinot.server.starter.helix;

import com.google.common.base.Preconditions;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now serving segment.
 * 3. Delete an existed segment.
 *
 * This only works for OFFLINE table segments and does not handle the CONSUMING state at all
 */
public class RealtimeSegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private final String _instanceId;
  private final InstanceDataManager _instanceDataManager;

  public RealtimeSegmentOnlineOfflineStateModelFactory(String instanceId, InstanceDataManager instanceDataManager) {
    _instanceId = instanceId;
    _instanceDataManager = instanceDataManager;
  }

  public static String getStateModelName() {
    return "RealtimeSegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new RealtimeSegmentOnlineOfflineStateModel();
  }


  // Helix seems to need StateModelInfo annotation for 'initialState'. It does not use the 'states' field.
  // The transitions in the helix messages indicate the from/to states, and helix uses the
  // Transition annotations (but only if StateModelInfo is defined).
  @SuppressWarnings("unused")
  @StateModelInfo(states = "{'OFFLINE', 'CONSUMING', 'ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class RealtimeSegmentOnlineOfflineStateModel extends StateModel {
    private final Logger _logger = LoggerFactory.getLogger(_instanceId + " - RealtimeSegmentOnlineOfflineStateModel");

    @Transition(from = "OFFLINE", to = "CONSUMING")
    public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeConsumingFromOffline() : " + message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();

      // TODO: This may not be needed if we split the state models between OFFLINE and REALTIME. Commented out for now
//      TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
//      Preconditions.checkNotNull(tableType);
//      if (tableType == TableType.OFFLINE) {
//        _logger.info("OFFLINE->CONSUMING state transition called for OFFLINE table, treat this as a no-op");
//        return;
//      }

      try {
        _instanceDataManager.addRealtimeSegment(realtimeTableName, segmentName);
      } catch (Exception e) {
        String errorMessage =
            String.format("Caught exception in state transition OFFLINE -> CONSUMING for table: %s, segment: %s",
                realtimeTableName, segmentName);
        _logger.error(errorMessage, e);
        TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(realtimeTableName);
        if (tableDataManager != null) {
          tableDataManager.addSegmentError(segmentName,
              new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
        }
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "CONSUMING", to = "ONLINE")
    public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeOnlineFromConsuming() : " + message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
      Preconditions.checkNotNull(tableType);

      // TODO: This may not be needed if we split the state models between OFFLINE and REALTIME. Commented out for now
//      if (tableType == TableType.OFFLINE) {
//        try {
//          _instanceDataManager.addOrReplaceSegment(realtimeTableName, segmentName);
//        } catch (Exception e) {
//          String errorMessage = String.format(
//              "Caught exception in state transition CONSUMING -> ONLINE for table: %s, segment: %s",
//              realtimeTableName, segmentName);
//          _logger.error(errorMessage, e);
//          TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(realtimeTableName);
//          if (tableDataManager != null) {
//            tableDataManager.addSegmentError(segmentName, new SegmentErrorInfo(System.currentTimeMillis(),
//                errorMessage, e));
//          }
//          Utils.rethrowException(e);
//        }
//      } else {
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(realtimeTableName);
      Preconditions.checkState(tableDataManager != null, "Failed to find table: %s", realtimeTableName);
      tableDataManager.onConsumingToOnline(segmentName);
      boolean isConsumingDone = false;
      while (!isConsumingDone) {
        SegmentDataManager acquiredSegment = tableDataManager.acquireSegment(segmentName);
        // For this transition to be correct in helix, we should already have a segment that is consuming
        Preconditions.checkState(acquiredSegment != null, "Failed to find segment: %s in table: %s", segmentName,
            realtimeTableName);

        // TODO: https://github.com/apache/pinot/issues/10049
        try {
          // This indicates that the realtime segment is already a completed immutable segment.
          // So nothing to do for this transition.
          if (!(acquiredSegment instanceof RealtimeSegmentDataManager)) {
            // We found an LLC segment that is not consuming right now, must be that we already swapped it with a
            // segment that has been built. Nothing to do for this state transition.
            _logger.info(
                "Segment {} not an instance of RealtimeSegmentDataManager. Reporting success for the transition",
                acquiredSegment.getSegmentName());
            return;
          }
          RealtimeSegmentDataManager segmentDataManager = (RealtimeSegmentDataManager) acquiredSegment;
          RealtimeSegmentDataManager.State state = segmentDataManager.getState();
          if ((!state.isFinal()) || state.shouldConsume()) {
            Thread.sleep(10_000L);
          } else {
            SegmentZKMetadata segmentZKMetadata =
                ZKMetadataProvider.getSegmentZKMetadata(_instanceDataManager.getPropertyStore(), realtimeTableName,
                    segmentName);
            segmentDataManager.goOnlineFromConsuming(segmentZKMetadata);
            isConsumingDone = true;
          }

//          // Use a single thread to wait for the consuming segment to be fully completed
//          ExecutorService executorService = Executors.newSingleThreadExecutor();
//          Future<Boolean> future = executorService.submit(() -> {
//            try {
//              while (true) {
//                RealtimeSegmentDataManager.State state = segmentDataManager.getState();
//                if (state.shouldConsume()) {
//                  Thread.sleep(5_000L);
//                } else {
//                  segmentDataManager.goOnlineFromConsuming(segmentZKMetadata);
//                  return true;
//                }
////              RealtimeSegmentDataManager.State state = segmentDataManager.getState();
////              if (state.isFinal()) {
////                return true;
////              }
//              }
//            } catch (InterruptedException e) {
//              throw new RuntimeException(e);
//            }
//          });
//          future.get();
//          executorService.shutdown();
        } catch (Exception e) {
          String errorMessage =
              String.format("Caught exception in state transition CONSUMING -> ONLINE for table: %s, segment: %s",
                  realtimeTableName, segmentName);
          _logger.error(errorMessage, e);
          tableDataManager.addSegmentError(segmentName,
              new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
          Utils.rethrowException(e);
        } finally {
          tableDataManager.releaseSegment(acquiredSegment);
        }
      }
    }

    @Transition(from = "CONSUMING", to = "OFFLINE")
    public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeOfflineFromConsuming() : " + message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.offloadSegment(realtimeTableName, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition CONSUMING -> OFFLINE for table: {}, segment: {}",
            realtimeTableName, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "CONSUMING", to = "DROPPED")
    public void onBecomeDroppedFromConsuming(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeDroppedFromConsuming() : " + message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(realtimeTableName);
      Preconditions.checkState(tableDataManager != null, "Failed to find table: %s", realtimeTableName);
      tableDataManager.onConsumingToDropped(segmentName);
      try {
        _instanceDataManager.offloadSegment(realtimeTableName, segmentName);
        _instanceDataManager.deleteSegment(realtimeTableName, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition CONSUMING -> DROPPED for table: {}, segment: {}",
            realtimeTableName, segmentName, e);
        Utils.rethrowException(e);
      }
    }


//    @Transition(from = "OFFLINE", to = "ONLINE")
//    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
//      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
//      String tableNameWithType = message.getResourceName();
//      String segmentName = message.getPartitionName();
//      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
//      Preconditions.checkArgument((tableType != null) && (tableType != TableType.REALTIME),
//          "TableType is null or is a REALTIME table, offline state model should not be called fo RT");
//      try {
//        _instanceDataManager.addOrReplaceSegment(tableNameWithType, segmentName);
//      } catch (Exception e) {
//        String errorMessage =
//            String.format("Caught exception in state transition OFFLINE -> ONLINE for table: %s, segment: %s",
//                tableNameWithType, segmentName);
//        _logger.error(errorMessage, e);
//        TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
//        if (tableDataManager != null) {
//          tableDataManager.addSegmentError(segmentName,
//              new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
//        }
//        Utils.rethrowException(e);
//      }
//    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.offloadSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition ONLINE -> OFFLINE for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition OFFLINE -> DROPPED for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.offloadSegment(tableNameWithType, segmentName);
        _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition ONLINE -> DROPPED for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeOfflineFromError() : " + message);
    }

    @Transition(from = "ERROR", to = "DROPPED")
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      _logger.info("RealtimeSegmentOnlineOfflineStateModel.onBecomeDroppedFromError() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition ERROR -> DROPPED for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }
  }
}
