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
public class LogConfig implements org.apache.thrift.TBase<LogConfig, LogConfig._Fields>, java.io.Serializable, Cloneable, Comparable<LogConfig> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LogConfig");

  private static final org.apache.thrift.protocol.TField NAMED_LOGGER_LEVEL_FIELD_DESC = new org.apache.thrift.protocol.TField("named_logger_level", org.apache.thrift.protocol.TType.MAP, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LogConfigStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LogConfigTupleSchemeFactory());
  }

  private Map<String,LogLevel> named_logger_level; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAMED_LOGGER_LEVEL((short)2, "named_logger_level");

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
        case 2: // NAMED_LOGGER_LEVEL
          return NAMED_LOGGER_LEVEL;
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
  private static final _Fields optionals[] = {_Fields.NAMED_LOGGER_LEVEL};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NAMED_LOGGER_LEVEL, new org.apache.thrift.meta_data.FieldMetaData("named_logger_level", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LogLevel.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LogConfig.class, metaDataMap);
  }

  public LogConfig() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LogConfig(LogConfig other) {
    if (other.is_set_named_logger_level()) {
      Map<String,LogLevel> __this__named_logger_level = new HashMap<String,LogLevel>(other.named_logger_level.size());
      for (Map.Entry<String, LogLevel> other_element : other.named_logger_level.entrySet()) {

        String other_element_key = other_element.getKey();
        LogLevel other_element_value = other_element.getValue();

        String __this__named_logger_level_copy_key = other_element_key;

        LogLevel __this__named_logger_level_copy_value = new LogLevel(other_element_value);

        __this__named_logger_level.put(__this__named_logger_level_copy_key, __this__named_logger_level_copy_value);
      }
      this.named_logger_level = __this__named_logger_level;
    }
  }

  public LogConfig deepCopy() {
    return new LogConfig(this);
  }

  @Override
  public void clear() {
    this.named_logger_level = null;
  }

  public int get_named_logger_level_size() {
    return (this.named_logger_level == null) ? 0 : this.named_logger_level.size();
  }

  public void put_to_named_logger_level(String key, LogLevel val) {
    if (this.named_logger_level == null) {
      this.named_logger_level = new HashMap<String,LogLevel>();
    }
    this.named_logger_level.put(key, val);
  }

  public Map<String,LogLevel> get_named_logger_level() {
    return this.named_logger_level;
  }

  public void set_named_logger_level(Map<String,LogLevel> named_logger_level) {
    this.named_logger_level = named_logger_level;
  }

  public void unset_named_logger_level() {
    this.named_logger_level = null;
  }

  /** Returns true if field named_logger_level is set (has been assigned a value) and false otherwise */
  public boolean is_set_named_logger_level() {
    return this.named_logger_level != null;
  }

  public void set_named_logger_level_isSet(boolean value) {
    if (!value) {
      this.named_logger_level = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NAMED_LOGGER_LEVEL:
      if (value == null) {
        unset_named_logger_level();
      } else {
        set_named_logger_level((Map<String,LogLevel>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NAMED_LOGGER_LEVEL:
      return get_named_logger_level();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NAMED_LOGGER_LEVEL:
      return is_set_named_logger_level();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LogConfig)
      return this.equals((LogConfig)that);
    return false;
  }

  public boolean equals(LogConfig that) {
    if (that == null)
      return false;

    boolean this_present_named_logger_level = true && this.is_set_named_logger_level();
    boolean that_present_named_logger_level = true && that.is_set_named_logger_level();
    if (this_present_named_logger_level || that_present_named_logger_level) {
      if (!(this_present_named_logger_level && that_present_named_logger_level))
        return false;
      if (!this.named_logger_level.equals(that.named_logger_level))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_named_logger_level = true && (is_set_named_logger_level());
    list.add(present_named_logger_level);
    if (present_named_logger_level)
      list.add(named_logger_level);

    return list.hashCode();
  }

  @Override
  public int compareTo(LogConfig other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_named_logger_level()).compareTo(other.is_set_named_logger_level());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_named_logger_level()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.named_logger_level, other.named_logger_level);
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
    StringBuilder sb = new StringBuilder("LogConfig(");
    boolean first = true;

    if (is_set_named_logger_level()) {
      sb.append("named_logger_level:");
      if (this.named_logger_level == null) {
        sb.append("null");
      } else {
        sb.append(this.named_logger_level);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

  private static class LogConfigStandardSchemeFactory implements SchemeFactory {
    public LogConfigStandardScheme getScheme() {
      return new LogConfigStandardScheme();
    }
  }

  private static class LogConfigStandardScheme extends StandardScheme<LogConfig> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LogConfig struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 2: // NAMED_LOGGER_LEVEL
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map654 = iprot.readMapBegin();
                struct.named_logger_level = new HashMap<String,LogLevel>(2*_map654.size);
                String _key655;
                LogLevel _val656;
                for (int _i657 = 0; _i657 < _map654.size; ++_i657)
                {
                  _key655 = iprot.readString();
                  _val656 = new LogLevel();
                  _val656.read(iprot);
                  struct.named_logger_level.put(_key655, _val656);
                }
                iprot.readMapEnd();
              }
              struct.set_named_logger_level_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LogConfig struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.named_logger_level != null) {
        if (struct.is_set_named_logger_level()) {
          oprot.writeFieldBegin(NAMED_LOGGER_LEVEL_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.named_logger_level.size()));
            for (Map.Entry<String, LogLevel> _iter658 : struct.named_logger_level.entrySet())
            {
              oprot.writeString(_iter658.getKey());
              _iter658.getValue().write(oprot);
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LogConfigTupleSchemeFactory implements SchemeFactory {
    public LogConfigTupleScheme getScheme() {
      return new LogConfigTupleScheme();
    }
  }

  private static class LogConfigTupleScheme extends TupleScheme<LogConfig> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LogConfig struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_named_logger_level()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.is_set_named_logger_level()) {
        {
          oprot.writeI32(struct.named_logger_level.size());
          for (Map.Entry<String, LogLevel> _iter659 : struct.named_logger_level.entrySet())
          {
            oprot.writeString(_iter659.getKey());
            _iter659.getValue().write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LogConfig struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map660 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.named_logger_level = new HashMap<String,LogLevel>(2*_map660.size);
          String _key661;
          LogLevel _val662;
          for (int _i663 = 0; _i663 < _map660.size; ++_i663)
          {
            _key661 = iprot.readString();
            _val662 = new LogLevel();
            _val662.read(iprot);
            struct.named_logger_level.put(_key661, _val662);
          }
        }
        struct.set_named_logger_level_isSet(true);
      }
    }
  }

}

