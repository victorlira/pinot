package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.bootstrap.StarTreeBootstrapJob;

public class AggregationKey {

  private static final Logger LOG = LoggerFactory
      .getLogger(AggregationKey.class);

  private String[] dimensionValues;

  public AggregationKey(String[] dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  public String[] getDimensionsValues() {
    return dimensionValues;
  }

  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    // write the number of dimensions
    out.writeInt(dimensionValues.length);
    // for each dimension write the length of each dimension followed by the
    // values
    for (String dimensionValue : dimensionValues) {
      byte[] bytes = dimensionValue.getBytes("UTF-8");
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    return baos.toByteArray();
  }

  public static AggregationKey fromBytes(byte[] bytes) throws IOException {
    long start = System.currentTimeMillis();
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    // read the number of dimensions
    int size = in.readInt();
    String[] dimensionValues = new String[size];
    // for each dimension write the length of each dimension followed by the
    // values
    for (int i = 0; i < size; i++) {
      int length = in.readInt();
      byte[] b = new byte[length];
      in.readFully(b);
      dimensionValues[i] = new String(b, "UTF-8");
    }
    long end = System.currentTimeMillis();
    LOG.info("deser time {}", (end - start));
    return new AggregationKey(dimensionValues);
  }

  public int compareTo(AggregationKey that) {
    // assumes both have the same length
    long start = System.currentTimeMillis();

    int length = Math.min(this.dimensionValues.length,
        that.dimensionValues.length);
    int ret = 0;
    for (int i = 0; i < length; i++) {
      ret = this.dimensionValues[i].compareTo(that.dimensionValues[i]);
      if (ret != 0) {
        break;
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("compare time {}", (end - start));
    return ret;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(dimensionValues);
  }

  @Override
  public boolean equals(Object obj) {
    // assumes both have the same length
    long start = System.currentTimeMillis();
    if (obj instanceof AggregationKey) {
      AggregationKey that = (AggregationKey) obj;
      int length = Math.min(this.dimensionValues.length,
          that.dimensionValues.length);
      boolean ret = true;
      for (int i = 0; i < length; i++) {
        ret = this.dimensionValues[i].equals(that.dimensionValues[i]);
        if (!ret) {
          break;
        }
      }
      long end = System.currentTimeMillis();
      LOG.info("equals time {}", (end - start));
      return ret;
    }
    return false;
  }

  @Override
  public String toString() {
    return dimensionValues.toString();
  }

  public static void main(String[] args) {
    String[] dimensionValues = new String[] { "", "chrome", "gmail.com",
        "android" };
    AggregationKey key = new AggregationKey(dimensionValues);
    System.out.println("tostring--" + key.toString());
  }

}