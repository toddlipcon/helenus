/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.facebook.infrastructure.service;

import com.facebook.thrift.TBase;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TField;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.protocol.TProtocolUtil;
import com.facebook.thrift.protocol.TStruct;
import com.facebook.thrift.protocol.TType;

public class column_t implements TBase, java.io.Serializable {
  public String columnName;
  public String value;
  public long timestamp;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean columnName = false;
    public boolean value = false;
    public boolean timestamp = false;
  }

  public column_t() {
  }

  public column_t(
    String columnName,
    String value,
    long timestamp)
  {
    this();
    this.columnName = columnName;
    this.__isset.columnName = true;
    this.value = value;
    this.__isset.value = true;
    this.timestamp = timestamp;
    this.__isset.timestamp = true;
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof column_t)
      return this.equals((column_t)that);
    return false;
  }

  public boolean equals(column_t that) {
    if (that == null)
      return false;

    boolean this_present_columnName = true && (this.columnName != null);
    boolean that_present_columnName = true && (that.columnName != null);
    if (this_present_columnName || that_present_columnName) {
      if (!(this_present_columnName && that_present_columnName))
        return false;
      if (!this.columnName.equals(that.columnName))
        return false;
    }

    boolean this_present_value = true && (this.value != null);
    boolean that_present_value = true && (that.value != null);
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!this.value.equals(that.value))
        return false;
    }

    boolean this_present_timestamp = true;
    boolean that_present_timestamp = true;
    if (this_present_timestamp || that_present_timestamp) {
      if (!(this_present_timestamp && that_present_timestamp))
        return false;
      if (this.timestamp != that.timestamp)
        return false;
    }

    return true;
  }

  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case 1:
          if (field.type == TType.STRING) {
            this.columnName = iprot.readString();
            this.__isset.columnName = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2:
          if (field.type == TType.STRING) {
            this.value = iprot.readString();
            this.__isset.value = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3:
          if (field.type == TType.I64) {
            this.timestamp = iprot.readI64();
            this.__isset.timestamp = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  public void write(TProtocol oprot) throws TException {
    TStruct struct = new TStruct("column_t");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.columnName != null) {
      field.name = "columnName";
      field.type = TType.STRING;
      field.id = 1;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.columnName);
      oprot.writeFieldEnd();
    }
    if (this.value != null) {
      field.name = "value";
      field.type = TType.STRING;
      field.id = 2;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.value);
      oprot.writeFieldEnd();
    }
    field.name = "timestamp";
    field.type = TType.I64;
    field.id = 3;
    oprot.writeFieldBegin(field);
    oprot.writeI64(this.timestamp);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("column_t(");
    sb.append("columnName:");
    sb.append(this.columnName);
    sb.append(",value:");
    sb.append(this.value);
    sb.append(",timestamp:");
    sb.append(this.timestamp);
    sb.append(")");
    return sb.toString();
  }

}

