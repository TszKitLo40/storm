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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-12-16")
public class NodeInfo implements org.apache.thrift.TBase<NodeInfo, NodeInfo._Fields>, java.io.Serializable, Cloneable, Comparable<NodeInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("NodeInfo");

  private static final org.apache.thrift.protocol.TField NODE_FIELD_DESC = new org.apache.thrift.protocol.TField("node", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("port", org.apache.thrift.protocol.TType.SET, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new NodeInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new NodeInfoTupleSchemeFactory());
  }

  private String node; // required
  private Set<Long> port; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NODE((short)1, "node"),
    PORT((short)2, "port");

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
        case 1: // NODE
          return NODE;
        case 2: // PORT
          return PORT;
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
    tmpMap.put(_Fields.NODE, new org.apache.thrift.meta_data.FieldMetaData("node", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PORT, new org.apache.thrift.meta_data.FieldMetaData("port", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.SetMetaData(org.apache.thrift.protocol.TType.SET, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(NodeInfo.class, metaDataMap);
  }

  public NodeInfo() {
  }

  public NodeInfo(
    String node,
    Set<Long> port)
  {
    this();
    this.node = node;
    this.port = port;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public NodeInfo(NodeInfo other) {
    if (other.is_set_node()) {
      this.node = other.node;
    }
    if (other.is_set_port()) {
      Set<Long> __this__port = new HashSet<Long>(other.port);
      this.port = __this__port;
    }
  }

  public NodeInfo deepCopy() {
    return new NodeInfo(this);
  }

  @Override
  public void clear() {
    this.node = null;
    this.port = null;
  }

  public String get_node() {
    return this.node;
  }

  public void set_node(String node) {
    this.node = node;
  }

  public void unset_node() {
    this.node = null;
  }

  /** Returns true if field node is set (has been assigned a value) and false otherwise */
  public boolean is_set_node() {
    return this.node != null;
  }

  public void set_node_isSet(boolean value) {
    if (!value) {
      this.node = null;
    }
  }

  public int get_port_size() {
    return (this.port == null) ? 0 : this.port.size();
  }

  public java.util.Iterator<Long> get_port_iterator() {
    return (this.port == null) ? null : this.port.iterator();
  }

  public void add_to_port(long elem) {
    if (this.port == null) {
      this.port = new HashSet<Long>();
    }
    this.port.add(elem);
  }

  public Set<Long> get_port() {
    return this.port;
  }

  public void set_port(Set<Long> port) {
    this.port = port;
  }

  public void unset_port() {
    this.port = null;
  }

  /** Returns true if field port is set (has been assigned a value) and false otherwise */
  public boolean is_set_port() {
    return this.port != null;
  }

  public void set_port_isSet(boolean value) {
    if (!value) {
      this.port = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NODE:
      if (value == null) {
        unset_node();
      } else {
        set_node((String)value);
      }
      break;

    case PORT:
      if (value == null) {
        unset_port();
      } else {
        set_port((Set<Long>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NODE:
      return get_node();

    case PORT:
      return get_port();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NODE:
      return is_set_node();
    case PORT:
      return is_set_port();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof NodeInfo)
      return this.equals((NodeInfo)that);
    return false;
  }

  public boolean equals(NodeInfo that) {
    if (that == null)
      return false;

    boolean this_present_node = true && this.is_set_node();
    boolean that_present_node = true && that.is_set_node();
    if (this_present_node || that_present_node) {
      if (!(this_present_node && that_present_node))
        return false;
      if (!this.node.equals(that.node))
        return false;
    }

    boolean this_present_port = true && this.is_set_port();
    boolean that_present_port = true && that.is_set_port();
    if (this_present_port || that_present_port) {
      if (!(this_present_port && that_present_port))
        return false;
      if (!this.port.equals(that.port))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_node = true && (is_set_node());
    list.add(present_node);
    if (present_node)
      list.add(node);

    boolean present_port = true && (is_set_port());
    list.add(present_port);
    if (present_port)
      list.add(port);

    return list.hashCode();
  }

  @Override
  public int compareTo(NodeInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_node()).compareTo(other.is_set_node());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_node()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.node, other.node);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_port()).compareTo(other.is_set_port());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_port()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.port, other.port);
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
    StringBuilder sb = new StringBuilder("NodeInfo(");
    boolean first = true;

    sb.append("node:");
    if (this.node == null) {
      sb.append("null");
    } else {
      sb.append(this.node);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("port:");
    if (this.port == null) {
      sb.append("null");
    } else {
      sb.append(this.port);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_node()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'node' is unset! Struct:" + toString());
    }

    if (!is_set_port()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'port' is unset! Struct:" + toString());
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

  private static class NodeInfoStandardSchemeFactory implements SchemeFactory {
    public NodeInfoStandardScheme getScheme() {
      return new NodeInfoStandardScheme();
    }
  }

  private static class NodeInfoStandardScheme extends StandardScheme<NodeInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, NodeInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NODE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.node = iprot.readString();
              struct.set_node_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.SET) {
              {
                org.apache.thrift.protocol.TSet _set516 = iprot.readSetBegin();
                struct.port = new HashSet<Long>(2*_set516.size);
                long _elem517;
                for (int _i518 = 0; _i518 < _set516.size; ++_i518)
                {
                  _elem517 = iprot.readI64();
                  struct.port.add(_elem517);
                }
                iprot.readSetEnd();
              }
              struct.set_port_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, NodeInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.node != null) {
        oprot.writeFieldBegin(NODE_FIELD_DESC);
        oprot.writeString(struct.node);
        oprot.writeFieldEnd();
      }
      if (struct.port != null) {
        oprot.writeFieldBegin(PORT_FIELD_DESC);
        {
          oprot.writeSetBegin(new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.I64, struct.port.size()));
          for (long _iter519 : struct.port)
          {
            oprot.writeI64(_iter519);
          }
          oprot.writeSetEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class NodeInfoTupleSchemeFactory implements SchemeFactory {
    public NodeInfoTupleScheme getScheme() {
      return new NodeInfoTupleScheme();
    }
  }

  private static class NodeInfoTupleScheme extends TupleScheme<NodeInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, NodeInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.node);
      {
        oprot.writeI32(struct.port.size());
        for (long _iter520 : struct.port)
        {
          oprot.writeI64(_iter520);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, NodeInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.node = iprot.readString();
      struct.set_node_isSet(true);
      {
        org.apache.thrift.protocol.TSet _set521 = new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.I64, iprot.readI32());
        struct.port = new HashSet<Long>(2*_set521.size);
        long _elem522;
        for (int _i523 = 0; _i523 < _set521.size; ++_i523)
        {
          _elem522 = iprot.readI64();
          struct.port.add(_elem522);
        }
      }
      struct.set_port_isSet(true);
    }
  }

}

