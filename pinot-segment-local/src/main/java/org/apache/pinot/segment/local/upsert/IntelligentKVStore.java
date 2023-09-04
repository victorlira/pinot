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

  public int size() {
    return _primaryKeyToRecordLocationMap.size() + _offheapStore.size();
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
    _primaryKeyToRecordLocationMap.putIfAbsent(key, (ConcurrentMapPartitionUpsertMetadataManager.RecordLocation) value);
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
    if (_primaryKeyToRecordLocationMap.containsKey(key)) {
      return computeIfPresentInternal(_primaryKeyToRecordLocationMap, key, remappingFunction);
    } else {
      return computeIfPresentInternal(_offheapStore, key, remappingFunction);
    }
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

    if (_primaryKeyToRecordLocationMap.containsKey(key)) {
      return computeInternal(_primaryKeyToRecordLocationMap, key, remappingFunction);
    } else {
      return computeInternal(_offheapStore, key, remappingFunction);
    }
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

  void forEach(BiConsumer<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> action) {
    //TODO: Extend this to offheap store as well
    forEachInternal(_primaryKeyToRecordLocationMap, action);
  }

  void forEachInternal(Map<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> map,
      BiConsumer<? super Object, ? super ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> action) {
    for (Map.Entry<Object, ConcurrentMapPartitionUpsertMetadataManager.RecordLocation> entry : map.entrySet()) {
      action.accept(entry.getKey(), entry.getValue());
    }
  }

  boolean remove(Object key,
      Object value) {

    if (_primaryKeyToRecordLocationMap.containsKey(key)) {
      return removeInternal(_primaryKeyToRecordLocationMap, key, value);
    }

    return removeInternal(_offheapStore, key, value);
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
}
