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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-11-12")
public class LSSupervisorAssignments implements org.apache.thrift.TBase<LSSupervisorAssignments, LSSupervisorAssignments._Fields>, java.io.Serializable, Cloneable, Comparable<LSSupervisorAssignments> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LSSupervisorAssignments");

  private static final org.apache.thrift.protocol.TField ASSIGNMENTS_FIELD_DESC = new org.apache.thrift.protocol.TField("assignments", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LSSupervisorAssignmentsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LSSupervisorAssignmentsTupleSchemeFactory());
  }

  private Map<Integer,LocalAssignment> assignments; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ASSIGNMENTS((short)1, "assignments");

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
        case 1: // ASSIGNMENTS
          return ASSIGNMENTS;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ASSIGNMENTS, new org.apache.thrift.meta_data.FieldMetaData("assignments", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LocalAssignment.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LSSupervisorAssignments.class, metaDataMap);
  }

  public LSSupervisorAssignments() {
  }

  public LSSupervisorAssignments(
    Map<Integer,LocalAssignment> assignments)
  {
    this();
    this.assignments = assignments;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LSSupervisorAssignments(LSSupervisorAssignments other) {
    if (other.is_set_assignments()) {
      Map<Integer,LocalAssignment> __this__assignments = new HashMap<Integer,LocalAssignment>(other.assignments.size());
      for (Map.Entry<Integer, LocalAssignment> other_element : other.assignments.entrySet()) {

        Integer other_element_key = other_element.getKey();
        LocalAssignment other_element_value = other_element.getValue();

        Integer __this__assignments_copy_key = other_element_key;

        LocalAssignment __this__assignments_copy_value = new LocalAssignment(other_element_value);

        __this__assignments.put(__this__assignments_copy_key, __this__assignments_copy_value);
      }
      this.assignments = __this__assignments;
    }
  }

  public LSSupervisorAssignments deepCopy() {
    return new LSSupervisorAssignments(this);
  }

  @Override
  public void clear() {
    this.assignments = null;
  }

  public int get_assignments_size() {
    return (this.assignments == null) ? 0 : this.assignments.size();
  }

  public void put_to_assignments(int key, LocalAssignment val) {
    if (this.assignments == null) {
      this.assignments = new HashMap<Integer,LocalAssignment>();
    }
    this.assignments.put(key, val);
  }

  public Map<Integer,LocalAssignment> get_assignments() {
    return this.assignments;
  }

  public void set_assignments(Map<Integer,LocalAssignment> assignments) {
    this.assignments = assignments;
  }

  public void unset_assignments() {
    this.assignments = null;
  }

  /** Returns true if field assignments is set (has been assigned a value) and false otherwise */
  public boolean is_set_assignments() {
    return this.assignments != null;
  }

  public void set_assignments_isSet(boolean value) {
    if (!value) {
      this.assignments = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ASSIGNMENTS:
      if (value == null) {
        unset_assignments();
      } else {
        set_assignments((Map<Integer,LocalAssignment>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ASSIGNMENTS:
      return get_assignments();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ASSIGNMENTS:
      return is_set_assignments();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LSSupervisorAssignments)
      return this.equals((LSSupervisorAssignments)that);
    return false;
  }

  public boolean equals(LSSupervisorAssignments that) {
    if (that == null)
      return false;

    boolean this_present_assignments = true && this.is_set_assignments();
    boolean that_present_assignments = true && that.is_set_assignments();
    if (this_present_assignments || that_present_assignments) {
      if (!(this_present_assignments && that_present_assignments))
        return false;
      if (!this.assignments.equals(that.assignments))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_assignments = true && (is_set_assignments());
    list.add(present_assignments);
    if (present_assignments)
      list.add(assignments);

    return list.hashCode();
  }

  @Override
  public int compareTo(LSSupervisorAssignments other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_assignments()).compareTo(other.is_set_assignments());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_assignments()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.assignments, other.assignments);
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
    StringBuilder sb = new StringBuilder("LSSupervisorAssignments(");
    boolean first = true;

    sb.append("assignments:");
    if (this.assignments == null) {
      sb.append("null");
    } else {
      sb.append(this.assignments);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_assignments()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'assignments' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
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

  private static class LSSupervisorAssignmentsStandardSchemeFactory implements SchemeFactory {
    public LSSupervisorAssignmentsStandardScheme getScheme() {
      return new LSSupervisorAssignmentsStandardScheme();
    }
  }

  private static class LSSupervisorAssignmentsStandardScheme extends StandardScheme<LSSupervisorAssignments> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LSSupervisorAssignments struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ASSIGNMENTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map636 = iprot.readMapBegin();
                struct.assignments = new HashMap<Integer,LocalAssignment>(2*_map636.size);
                int _key637;
                LocalAssignment _val638;
                for (int _i639 = 0; _i639 < _map636.size; ++_i639)
                {
                  _key637 = iprot.readI32();
                  _val638 = new LocalAssignment();
                  _val638.read(iprot);
                  struct.assignments.put(_key637, _val638);
                }
                iprot.readMapEnd();
              }
              struct.set_assignments_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LSSupervisorAssignments struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.assignments != null) {
        oprot.writeFieldBegin(ASSIGNMENTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.STRUCT, struct.assignments.size()));
          for (Map.Entry<Integer, LocalAssignment> _iter640 : struct.assignments.entrySet())
          {
            oprot.writeI32(_iter640.getKey());
            _iter640.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LSSupervisorAssignmentsTupleSchemeFactory implements SchemeFactory {
    public LSSupervisorAssignmentsTupleScheme getScheme() {
      return new LSSupervisorAssignmentsTupleScheme();
    }
  }

  private static class LSSupervisorAssignmentsTupleScheme extends TupleScheme<LSSupervisorAssignments> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LSSupervisorAssignments struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.assignments.size());
        for (Map.Entry<Integer, LocalAssignment> _iter641 : struct.assignments.entrySet())
        {
          oprot.writeI32(_iter641.getKey());
          _iter641.getValue().write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LSSupervisorAssignments struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map642 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.assignments = new HashMap<Integer,LocalAssignment>(2*_map642.size);
        int _key643;
        LocalAssignment _val644;
        for (int _i645 = 0; _i645 < _map642.size; ++_i645)
        {
          _key643 = iprot.readI32();
          _val644 = new LocalAssignment();
          _val644.read(iprot);
          struct.assignments.put(_key643, _val644);
        }
      }
      struct.set_assignments_isSet(true);
    }
  }

}

