/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package backtype.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2016-1-27")
public class ExecutorAggregateStats implements org.apache.thrift.TBase<ExecutorAggregateStats, ExecutorAggregateStats._Fields>, java.io.Serializable, Cloneable, Comparable<ExecutorAggregateStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExecutorAggregateStats");

  private static final org.apache.thrift.protocol.TField EXEC_SUMMARY_FIELD_DESC = new org.apache.thrift.protocol.TField("exec_summary", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField STATS_FIELD_DESC = new org.apache.thrift.protocol.TField("stats", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ExecutorAggregateStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ExecutorAggregateStatsTupleSchemeFactory());
  }

  private ExecutorSummary exec_summary; // optional
  private ComponentAggregateStats stats; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EXEC_SUMMARY((short)1, "exec_summary"),
    STATS((short)2, "stats");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // EXEC_SUMMARY
          return EXEC_SUMMARY;
        case 2: // STATS
          return STATS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.EXEC_SUMMARY,_Fields.STATS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.EXEC_SUMMARY, new org.apache.thrift.meta_data.FieldMetaData("exec_summary", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ExecutorSummary.class)));
    tmpMap.put(_Fields.STATS, new org.apache.thrift.meta_data.FieldMetaData("stats", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ComponentAggregateStats.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExecutorAggregateStats.class, metaDataMap);
  }

  public ExecutorAggregateStats() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExecutorAggregateStats(ExecutorAggregateStats other) {
    if (other.is_set_exec_summary()) {
      this.exec_summary = new ExecutorSummary(other.exec_summary);
    }
    if (other.is_set_stats()) {
      this.stats = new ComponentAggregateStats(other.stats);
    }
  }

  public ExecutorAggregateStats deepCopy() {
    return new ExecutorAggregateStats(this);
  }

  @Override
  public void clear() {
    this.exec_summary = null;
    this.stats = null;
  }

  public ExecutorSummary get_exec_summary() {
    return this.exec_summary;
  }

  public void set_exec_summary(ExecutorSummary exec_summary) {
    this.exec_summary = exec_summary;
  }

  public void unset_exec_summary() {
    this.exec_summary = null;
  }

  /** Returns true if field exec_summary is set (has been assigned a value) and false otherwise */
  public boolean is_set_exec_summary() {
    return this.exec_summary != null;
  }

  public void set_exec_summary_isSet(boolean value) {
    if (!value) {
      this.exec_summary = null;
    }
  }

  public ComponentAggregateStats get_stats() {
    return this.stats;
  }

  public void set_stats(ComponentAggregateStats stats) {
    this.stats = stats;
  }

  public void unset_stats() {
    this.stats = null;
  }

  /** Returns true if field stats is set (has been assigned a value) and false otherwise */
  public boolean is_set_stats() {
    return this.stats != null;
  }

  public void set_stats_isSet(boolean value) {
    if (!value) {
      this.stats = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case EXEC_SUMMARY:
      if (value == null) {
        unset_exec_summary();
      } else {
        set_exec_summary((ExecutorSummary)value);
      }
      break;

    case STATS:
      if (value == null) {
        unset_stats();
      } else {
        set_stats((ComponentAggregateStats)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case EXEC_SUMMARY:
      return get_exec_summary();

    case STATS:
      return get_stats();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case EXEC_SUMMARY:
      return is_set_exec_summary();
    case STATS:
      return is_set_stats();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ExecutorAggregateStats)
      return this.equals((ExecutorAggregateStats)that);
    return false;
  }

  public boolean equals(ExecutorAggregateStats that) {
    if (that == null)
      return false;

    boolean this_present_exec_summary = true && this.is_set_exec_summary();
    boolean that_present_exec_summary = true && that.is_set_exec_summary();
    if (this_present_exec_summary || that_present_exec_summary) {
      if (!(this_present_exec_summary && that_present_exec_summary))
        return false;
      if (!this.exec_summary.equals(that.exec_summary))
        return false;
    }

    boolean this_present_stats = true && this.is_set_stats();
    boolean that_present_stats = true && that.is_set_stats();
    if (this_present_stats || that_present_stats) {
      if (!(this_present_stats && that_present_stats))
        return false;
      if (!this.stats.equals(that.stats))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_exec_summary = true && (is_set_exec_summary());
    list.add(present_exec_summary);
    if (present_exec_summary)
      list.add(exec_summary);

    boolean present_stats = true && (is_set_stats());
    list.add(present_stats);
    if (present_stats)
      list.add(stats);

    return list.hashCode();
  }

  @Override
  public int compareTo(ExecutorAggregateStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_exec_summary()).compareTo(other.is_set_exec_summary());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_exec_summary()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.exec_summary, other.exec_summary);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_stats()).compareTo(other.is_set_stats());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_stats()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stats, other.stats);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ExecutorAggregateStats(");
    boolean first = true;

    if (is_set_exec_summary()) {
      sb.append("exec_summary:");
      if (this.exec_summary == null) {
        sb.append("null");
      } else {
        sb.append(this.exec_summary);
      }
      first = false;
    }
    if (is_set_stats()) {
      if (!first) sb.append(", ");
      sb.append("stats:");
      if (this.stats == null) {
        sb.append("null");
      } else {
        sb.append(this.stats);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (exec_summary != null) {
      exec_summary.validate();
    }
    if (stats != null) {
      stats.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExecutorAggregateStatsStandardSchemeFactory implements SchemeFactory {
    public ExecutorAggregateStatsStandardScheme getScheme() {
      return new ExecutorAggregateStatsStandardScheme();
    }
  }

  private static class ExecutorAggregateStatsStandardScheme extends StandardScheme<ExecutorAggregateStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExecutorAggregateStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // EXEC_SUMMARY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.exec_summary = new ExecutorSummary();
              struct.exec_summary.read(iprot);
              struct.set_exec_summary_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STATS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.stats = new ComponentAggregateStats();
              struct.stats.read(iprot);
              struct.set_stats_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExecutorAggregateStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.exec_summary != null) {
        if (struct.is_set_exec_summary()) {
          oprot.writeFieldBegin(EXEC_SUMMARY_FIELD_DESC);
          struct.exec_summary.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.stats != null) {
        if (struct.is_set_stats()) {
          oprot.writeFieldBegin(STATS_FIELD_DESC);
          struct.stats.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExecutorAggregateStatsTupleSchemeFactory implements SchemeFactory {
    public ExecutorAggregateStatsTupleScheme getScheme() {
      return new ExecutorAggregateStatsTupleScheme();
    }
  }

  private static class ExecutorAggregateStatsTupleScheme extends TupleScheme<ExecutorAggregateStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExecutorAggregateStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_exec_summary()) {
        optionals.set(0);
      }
      if (struct.is_set_stats()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.is_set_exec_summary()) {
        struct.exec_summary.write(oprot);
      }
      if (struct.is_set_stats()) {
        struct.stats.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExecutorAggregateStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.exec_summary = new ExecutorSummary();
        struct.exec_summary.read(iprot);
        struct.set_exec_summary_isSet(true);
      }
      if (incoming.get(1)) {
        struct.stats = new ComponentAggregateStats();
        struct.stats.read(iprot);
        struct.set_stats_isSet(true);
      }
    }
  }

}

