// automatically generated by the FlatBuffers compiler, do not modify

package com.bushpath.nfennel.flatbuffers;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class DataRequest extends Table {
  public static DataRequest getRootAsDataRequest(ByteBuffer _bb) { return getRootAsDataRequest(_bb, new DataRequest()); }
  public static DataRequest getRootAsDataRequest(ByteBuffer _bb, DataRequest obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public DataRequest __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long id() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }

  public static int createDataRequest(FlatBufferBuilder builder,
      long id) {
    builder.startObject(1);
    DataRequest.addId(builder, id);
    return DataRequest.endDataRequest(builder);
  }

  public static void startDataRequest(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addId(FlatBufferBuilder builder, long id) { builder.addLong(0, id, 0L); }
  public static int endDataRequest(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

