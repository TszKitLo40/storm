#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Autogenerated by Thrift Compiler (0.9.2)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:utf8strings
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def getAllHostNames(self):
    pass

  def migrateTasks(self, originalHostName, targetHostName, taskId, routeNo):
    """
    Parameters:
     - originalHostName
     - targetHostName
     - taskId
     - routeNo
    """
    pass

  def createRouting(self, hostName, taskid, routeNo, type):
    """
    Parameters:
     - hostName
     - taskid
     - routeNo
     - type
    """
    pass

  def withdrawRemoteRoute(self, remoteHostName, taskid, route):
    """
    Parameters:
     - remoteHostName
     - taskid
     - route
    """
    pass

  def reportTaskThroughput(self, taskid):
    """
    Parameters:
     - taskid
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def getAllHostNames(self):
    self.send_getAllHostNames()
    return self.recv_getAllHostNames()

  def send_getAllHostNames(self):
    self._oprot.writeMessageBegin('getAllHostNames', TMessageType.CALL, self._seqid)
    args = getAllHostNames_args()
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_getAllHostNames(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = getAllHostNames_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    raise TApplicationException(TApplicationException.MISSING_RESULT, "getAllHostNames failed: unknown result");

  def migrateTasks(self, originalHostName, targetHostName, taskId, routeNo):
    """
    Parameters:
     - originalHostName
     - targetHostName
     - taskId
     - routeNo
    """
    self.send_migrateTasks(originalHostName, targetHostName, taskId, routeNo)
    self.recv_migrateTasks()

  def send_migrateTasks(self, originalHostName, targetHostName, taskId, routeNo):
    self._oprot.writeMessageBegin('migrateTasks', TMessageType.CALL, self._seqid)
    args = migrateTasks_args()
    args.originalHostName = originalHostName
    args.targetHostName = targetHostName
    args.taskId = taskId
    args.routeNo = routeNo
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_migrateTasks(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = migrateTasks_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.me is not None:
      raise result.me
    return

  def createRouting(self, hostName, taskid, routeNo, type):
    """
    Parameters:
     - hostName
     - taskid
     - routeNo
     - type
    """
    self.send_createRouting(hostName, taskid, routeNo, type)
    self.recv_createRouting()

  def send_createRouting(self, hostName, taskid, routeNo, type):
    self._oprot.writeMessageBegin('createRouting', TMessageType.CALL, self._seqid)
    args = createRouting_args()
    args.hostName = hostName
    args.taskid = taskid
    args.routeNo = routeNo
    args.type = type
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_createRouting(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = createRouting_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.hmee is not None:
      raise result.hmee
    return

  def withdrawRemoteRoute(self, remoteHostName, taskid, route):
    """
    Parameters:
     - remoteHostName
     - taskid
     - route
    """
    self.send_withdrawRemoteRoute(remoteHostName, taskid, route)
    self.recv_withdrawRemoteRoute()

  def send_withdrawRemoteRoute(self, remoteHostName, taskid, route):
    self._oprot.writeMessageBegin('withdrawRemoteRoute', TMessageType.CALL, self._seqid)
    args = withdrawRemoteRoute_args()
    args.remoteHostName = remoteHostName
    args.taskid = taskid
    args.route = route
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_withdrawRemoteRoute(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = withdrawRemoteRoute_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.e is not None:
      raise result.e
    if result.hnee is not None:
      raise result.hnee
    return

  def reportTaskThroughput(self, taskid):
    """
    Parameters:
     - taskid
    """
    self.send_reportTaskThroughput(taskid)
    return self.recv_reportTaskThroughput()

  def send_reportTaskThroughput(self, taskid):
    self._oprot.writeMessageBegin('reportTaskThroughput', TMessageType.CALL, self._seqid)
    args = reportTaskThroughput_args()
    args.taskid = taskid
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_reportTaskThroughput(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = reportTaskThroughput_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.tnee is not None:
      raise result.tnee
    raise TApplicationException(TApplicationException.MISSING_RESULT, "reportTaskThroughput failed: unknown result");


class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["getAllHostNames"] = Processor.process_getAllHostNames
    self._processMap["migrateTasks"] = Processor.process_migrateTasks
    self._processMap["createRouting"] = Processor.process_createRouting
    self._processMap["withdrawRemoteRoute"] = Processor.process_withdrawRemoteRoute
    self._processMap["reportTaskThroughput"] = Processor.process_reportTaskThroughput

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_getAllHostNames(self, seqid, iprot, oprot):
    args = getAllHostNames_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = getAllHostNames_result()
    result.success = self._handler.getAllHostNames()
    oprot.writeMessageBegin("getAllHostNames", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_migrateTasks(self, seqid, iprot, oprot):
    args = migrateTasks_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = migrateTasks_result()
    try:
      self._handler.migrateTasks(args.originalHostName, args.targetHostName, args.taskId, args.routeNo)
    except MigrationException, me:
      result.me = me
    oprot.writeMessageBegin("migrateTasks", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_createRouting(self, seqid, iprot, oprot):
    args = createRouting_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = createRouting_result()
    try:
      self._handler.createRouting(args.hostName, args.taskid, args.routeNo, args.type)
    except HostNotExistException, hmee:
      result.hmee = hmee
    oprot.writeMessageBegin("createRouting", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_withdrawRemoteRoute(self, seqid, iprot, oprot):
    args = withdrawRemoteRoute_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = withdrawRemoteRoute_result()
    try:
      self._handler.withdrawRemoteRoute(args.remoteHostName, args.taskid, args.route)
    except TaskNotExistException, e:
      result.e = e
    except HostNotExistException, hnee:
      result.hnee = hnee
    oprot.writeMessageBegin("withdrawRemoteRoute", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_reportTaskThroughput(self, seqid, iprot, oprot):
    args = reportTaskThroughput_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = reportTaskThroughput_result()
    try:
      result.success = self._handler.reportTaskThroughput(args.taskid)
    except TaskNotExistException, tnee:
      result.tnee = tnee
    oprot.writeMessageBegin("reportTaskThroughput", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class getAllHostNames_args:

  thrift_spec = (
  )

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('getAllHostNames_args')
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class getAllHostNames_result:
  """
  Attributes:
   - success
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRING,None), None, ), # 0
  )

  def __init__(self, success=None,):
    self.success = success

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype597, _size594) = iprot.readListBegin()
          for _i598 in xrange(_size594):
            _elem599 = iprot.readString().decode('utf-8')
            self.success.append(_elem599)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('getAllHostNames_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRING, len(self.success))
      for iter600 in self.success:
        oprot.writeString(iter600.encode('utf-8'))
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.success)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class migrateTasks_args:
  """
  Attributes:
   - originalHostName
   - targetHostName
   - taskId
   - routeNo
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'originalHostName', None, None, ), # 1
    (2, TType.STRING, 'targetHostName', None, None, ), # 2
    (3, TType.I32, 'taskId', None, None, ), # 3
    (4, TType.I32, 'routeNo', None, None, ), # 4
  )

  def __init__(self, originalHostName=None, targetHostName=None, taskId=None, routeNo=None,):
    self.originalHostName = originalHostName
    self.targetHostName = targetHostName
    self.taskId = taskId
    self.routeNo = routeNo

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.originalHostName = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.targetHostName = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.taskId = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I32:
          self.routeNo = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('migrateTasks_args')
    if self.originalHostName is not None:
      oprot.writeFieldBegin('originalHostName', TType.STRING, 1)
      oprot.writeString(self.originalHostName.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.targetHostName is not None:
      oprot.writeFieldBegin('targetHostName', TType.STRING, 2)
      oprot.writeString(self.targetHostName.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.taskId is not None:
      oprot.writeFieldBegin('taskId', TType.I32, 3)
      oprot.writeI32(self.taskId)
      oprot.writeFieldEnd()
    if self.routeNo is not None:
      oprot.writeFieldBegin('routeNo', TType.I32, 4)
      oprot.writeI32(self.routeNo)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.originalHostName)
    value = (value * 31) ^ hash(self.targetHostName)
    value = (value * 31) ^ hash(self.taskId)
    value = (value * 31) ^ hash(self.routeNo)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class migrateTasks_result:
  """
  Attributes:
   - me
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'me', (MigrationException, MigrationException.thrift_spec), None, ), # 1
  )

  def __init__(self, me=None,):
    self.me = me

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.me = MigrationException()
          self.me.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('migrateTasks_result')
    if self.me is not None:
      oprot.writeFieldBegin('me', TType.STRUCT, 1)
      self.me.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.me)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class createRouting_args:
  """
  Attributes:
   - hostName
   - taskid
   - routeNo
   - type
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'hostName', None, None, ), # 1
    (2, TType.I32, 'taskid', None, None, ), # 2
    (3, TType.I32, 'routeNo', None, None, ), # 3
    (4, TType.STRING, 'type', None, None, ), # 4
  )

  def __init__(self, hostName=None, taskid=None, routeNo=None, type=None,):
    self.hostName = hostName
    self.taskid = taskid
    self.routeNo = routeNo
    self.type = type

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.hostName = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.taskid = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.routeNo = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.type = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('createRouting_args')
    if self.hostName is not None:
      oprot.writeFieldBegin('hostName', TType.STRING, 1)
      oprot.writeString(self.hostName.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.taskid is not None:
      oprot.writeFieldBegin('taskid', TType.I32, 2)
      oprot.writeI32(self.taskid)
      oprot.writeFieldEnd()
    if self.routeNo is not None:
      oprot.writeFieldBegin('routeNo', TType.I32, 3)
      oprot.writeI32(self.routeNo)
      oprot.writeFieldEnd()
    if self.type is not None:
      oprot.writeFieldBegin('type', TType.STRING, 4)
      oprot.writeString(self.type.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.hostName)
    value = (value * 31) ^ hash(self.taskid)
    value = (value * 31) ^ hash(self.routeNo)
    value = (value * 31) ^ hash(self.type)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class createRouting_result:
  """
  Attributes:
   - hmee
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'hmee', (HostNotExistException, HostNotExistException.thrift_spec), None, ), # 1
  )

  def __init__(self, hmee=None,):
    self.hmee = hmee

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.hmee = HostNotExistException()
          self.hmee.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('createRouting_result')
    if self.hmee is not None:
      oprot.writeFieldBegin('hmee', TType.STRUCT, 1)
      self.hmee.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.hmee)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class withdrawRemoteRoute_args:
  """
  Attributes:
   - remoteHostName
   - taskid
   - route
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'remoteHostName', None, None, ), # 1
    (2, TType.I32, 'taskid', None, None, ), # 2
    (3, TType.I32, 'route', None, None, ), # 3
  )

  def __init__(self, remoteHostName=None, taskid=None, route=None,):
    self.remoteHostName = remoteHostName
    self.taskid = taskid
    self.route = route

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.remoteHostName = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.taskid = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.route = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('withdrawRemoteRoute_args')
    if self.remoteHostName is not None:
      oprot.writeFieldBegin('remoteHostName', TType.STRING, 1)
      oprot.writeString(self.remoteHostName.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.taskid is not None:
      oprot.writeFieldBegin('taskid', TType.I32, 2)
      oprot.writeI32(self.taskid)
      oprot.writeFieldEnd()
    if self.route is not None:
      oprot.writeFieldBegin('route', TType.I32, 3)
      oprot.writeI32(self.route)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.remoteHostName)
    value = (value * 31) ^ hash(self.taskid)
    value = (value * 31) ^ hash(self.route)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class withdrawRemoteRoute_result:
  """
  Attributes:
   - e
   - hnee
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (TaskNotExistException, TaskNotExistException.thrift_spec), None, ), # 1
    (2, TType.STRUCT, 'hnee', (HostNotExistException, HostNotExistException.thrift_spec), None, ), # 2
  )

  def __init__(self, e=None, hnee=None,):
    self.e = e
    self.hnee = hnee

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = TaskNotExistException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.hnee = HostNotExistException()
          self.hnee.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('withdrawRemoteRoute_result')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    if self.hnee is not None:
      oprot.writeFieldBegin('hnee', TType.STRUCT, 2)
      self.hnee.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.e)
    value = (value * 31) ^ hash(self.hnee)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class reportTaskThroughput_args:
  """
  Attributes:
   - taskid
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'taskid', None, None, ), # 1
  )

  def __init__(self, taskid=None,):
    self.taskid = taskid

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.taskid = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('reportTaskThroughput_args')
    if self.taskid is not None:
      oprot.writeFieldBegin('taskid', TType.I32, 1)
      oprot.writeI32(self.taskid)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.taskid)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class reportTaskThroughput_result:
  """
  Attributes:
   - success
   - tnee
  """

  thrift_spec = (
    (0, TType.DOUBLE, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'tnee', (TaskNotExistException, TaskNotExistException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, tnee=None,):
    self.success = success
    self.tnee = tnee

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.DOUBLE:
          self.success = iprot.readDouble();
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.tnee = TaskNotExistException()
          self.tnee.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('reportTaskThroughput_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.DOUBLE, 0)
      oprot.writeDouble(self.success)
      oprot.writeFieldEnd()
    if self.tnee is not None:
      oprot.writeFieldBegin('tnee', TType.STRUCT, 1)
      self.tnee.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.success)
    value = (value * 31) ^ hash(self.tnee)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
