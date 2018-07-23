// automatically generated by the FlatBuffers compiler, do not modify

package com.bushpath.nfennel.flatbuffers;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Failure extends Table {
  public static Failure getRootAsFailure(ByteBuffer _bb) { return getRootAsFailure(_bb, new Failure()); }
  public static Failure getRootAsFailure(ByteBuffer _bb, Failure obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public Failure __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String message() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer messageAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer messageInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }

  public static int createFailure(FlatBufferBuilder builder,
      int messageOffset) {
    builder.startObject(1);
    Failure.addMessage(builder, messageOffset);
    return Failure.endFailure(builder);
  }

  public static void startFailure(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addMessage(FlatBufferBuilder builder, int messageOffset) { builder.addOffset(0, messageOffset, 0); }
  public static int endFailure(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

