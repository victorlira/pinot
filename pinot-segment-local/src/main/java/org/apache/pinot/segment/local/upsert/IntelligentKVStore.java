package org.apache.pinot.segment.local.upsert;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;


public class IntelligentKVStore {
  @VisibleForTesting
  final ConcurrentHashMap<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation>
      _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();
  final Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> _offheapStore;

  public IntelligentKVStore(Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> offheapStore) {
    _offheapStore = offheapStore;
  }

  public int sizeOfPrimaryStore() {
    return _primaryKeyToRecordLocationMap.size();
  }

  public int sizeOfSecondaryStore() {
    return _offheapStore.size();
  }

  public boolean isEmpty() {
    return _primaryKeyToRecordLocationMap.isEmpty() && _offheapStore.isEmpty();
  }

  public boolean containsKey(Object key) {
    return _primaryKeyToRecordLocationMap.containsKey(key) || _offheapStore.containsKey(key);
  }

  public boolean containsValue(Object value) {
    return _primaryKeyToRecordLocationMap.containsValue(value) || _offheapStore.containsValue(value);
  }

  public ConcurrentMapPartitionUpsertMetadataManager.RecordLocation get(Object key) {
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation value = _primaryKeyToRecordLocationMap.get(key);

    if (value == null) {
      return _offheapStore.get(key);
    }

    return value;
  }

  public Object put(Object key, Object value) {

    if (!(value instanceof ConcurrentMapPartitionUpsertMetadataManager.RecordLocation)) {
      throw new IllegalArgumentException("Value should be of type RecordLocation");
    }

    // We do a double put
    _primaryKeyToRecordLocationMap.put(key, (ConcurrentMapPartitionUpsertMetadataManager.RecordLocation) value);
    _offheapStore.put(key, (ConcurrentMapPartitionUpsertMetadataManager.RecordLocation) value);

    return value;
  }

  public Object remove(Object key) {
    // Remove in all maps where the key is present
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation firstValue = _primaryKeyToRecordLocationMap.remove(key);
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation secondValue = _offheapStore.remove(key);

    if (firstValue == null) {
      return secondValue;
    }

    return firstValue;
  }

  public void clear() {
    _primaryKeyToRecordLocationMap.clear();
    _offheapStore.clear();
  }

  ConcurrentMapPartitionUpsertMetadataManager.RecordLocation computeIfPresent(Object key,
      BiFunction<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation, ?
          extends ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> remappingFunction) {
    // TODO: Concurrency checks
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation primaryStoreLocation = null;
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation offHeapStoreLocation = null;

    if (_primaryKeyToRecordLocationMap.containsKey(key)) {
      primaryStoreLocation = computeIfPresentInternal(_primaryKeyToRecordLocationMap, key, remappingFunction);
    }

    if (_offheapStore.containsKey(key)) {
      offHeapStoreLocation = computeIfPresentInternal(_offheapStore, key, remappingFunction);
    }

    return comparePrimaryAndOffheapValues(primaryStoreLocation, offHeapStoreLocation);
  }

  ConcurrentMapPartitionUpsertMetadataManager.RecordLocation computeIfPresentInternal(
      Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> map, Object key,
      BiFunction<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation, ?
          extends ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> remappingFunction) {
    if (map.get(key) != null) {
      ConcurrentMapPartitionUpsertMetadataManager.RecordLocation oldValue = map.get(key);
      ConcurrentMapPartitionUpsertMetadataManager.RecordLocation newValue = remappingFunction.apply(key, oldValue);
      if (newValue != null) {
        map.put(key, newValue);
        return newValue;
      } else {
        map.remove(key);
        return null;
      }
    }

    return null;
  }

  ConcurrentMapPartitionUpsertMetadataManager.RecordLocation compute(Object key,
      BiFunction<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation, ?
          extends ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> remappingFunction) {

    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation primaryStoreLocation = null;
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation offHeapStoreLocation = null;

      primaryStoreLocation = computeInternal(_primaryKeyToRecordLocationMap, key, remappingFunction);

      offHeapStoreLocation = computeInternal(_offheapStore, key, remappingFunction);

    return comparePrimaryAndOffheapValues(primaryStoreLocation, offHeapStoreLocation);
  }

  ConcurrentMapPartitionUpsertMetadataManager.RecordLocation computeInternal(
      Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> map, Object key,
      BiFunction<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation, ?
          extends ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> remappingFunction) {
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation oldValue = map.get(key);
    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation newValue = remappingFunction.apply(key, oldValue);
    if (oldValue != null) {
      if (newValue != null) {
        map.put(key, newValue);

        return newValue;
      } else {
        map.remove(key);

        return null;
      }
    } else {
      if (newValue != null) {
        map.put(key, newValue);
      }
      return null;
    }
  }

  void forEachOnPrimaryDataStore(
      BiConsumer<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> action) {
    forEachInternal(_primaryKeyToRecordLocationMap, action);
  }

  void forEachOnOffheapDataStore(
      BiConsumer<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> action) {
    forEachInternal(_offheapStore, action);
  }

  void forEach(BiConsumer<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> action) {
    //TODO: Make these two operations concurrent?
    forEachOnPrimaryDataStore(action);
    forEachOnOffheapDataStore(action);
  }

  void forEachInternal(Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> map,
      BiConsumer<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> action) {
    for (Map.Entry<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> entry : map.entrySet()) {
      action.accept(entry.getKey(), entry.getValue());
    }
  }

  ConcurrentMapPartitionUpsertMetadataManager.RecordLocation comparePrimaryAndOffheapValues(
      ConcurrentMapPartitionUpsertMetadataManager.RecordLocation primaryStoreLocation,
      ConcurrentMapPartitionUpsertMetadataManager.RecordLocation offHeapStoreLocation) {
    if (primaryStoreLocation == null) {
      return offHeapStoreLocation;
    }

    if (offHeapStoreLocation == null) {
      return primaryStoreLocation;
    }

    // If both are null, we would have handled it in the above conditions

    if (primaryStoreLocation.getComparisonValue().compareTo(offHeapStoreLocation.getComparisonValue()) >= 0) {
      return primaryStoreLocation;
    }

    return offHeapStoreLocation;
  }

  boolean remove(Object key, Object value) {
    boolean isRemoved = false;
    if (_primaryKeyToRecordLocationMap.containsKey(key)) {
      removeInternal(_primaryKeyToRecordLocationMap, key, value);
      isRemoved = true;
    }

    if (_offheapStore.containsKey(key)) {
      removeInternal(_offheapStore, key, value);
      isRemoved = true;
    }

    return isRemoved;
  }

  boolean removeInternal(Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> map, Object key,
      Object value) {
    if (map.containsKey(key) && Objects.equals(map.get(key), value)) {
      map.remove(key);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Here is how this method works:
   *
   * If primary store does not contain the key, then no action needs to be taken
   *
   * If primary store contains the key, then the first thing we do is to perform
   * the write on the offheap store. Once the offheap store write is completed,
   * we do another read on the primary store and check if the previously known
   * value for this key from the primary store is the same.
   * If yes, then delete the value from the primary store.
   * If not, then do nothing. This will ensure that hot keys are still kept
   * in primary store and the key gets naturally removed from the primary
   * store when it cools down.
   *
   * There is a caveat here -- if majority keys are hot, then we are essentially
   * keeping a duplicate for the key on disk. The invariant
   * here is that the update rate of keys after the set "hot" TTL goes down
   * drastically.
   *
   * Once a key successfully moves to off heap only, all updates are percolated
   * directly to the off heap store.
   * @param key
   */
  void transferKey(Object key) {

    if (!(_primaryKeyToRecordLocationMap.containsKey(key))) {
      return;
    }

    ConcurrentMapPartitionUpsertMetadataManager.RecordLocation currentRecordLocation =
        _primaryKeyToRecordLocationMap.get(key);
    _offheapStore.put(key, currentRecordLocation);

    // If we got here, assuming that the write went through
    if (_primaryKeyToRecordLocationMap.get(key).getComparisonValue()
        .compareTo(currentRecordLocation.getComparisonValue()) == 0) {
      // Equal values, no updates. Remove from the primary store
      // NOTE: There is a race condition here that the value can change
      // between the read above to here.
      _primaryKeyToRecordLocationMap.remove(key);
    }
  }
}
