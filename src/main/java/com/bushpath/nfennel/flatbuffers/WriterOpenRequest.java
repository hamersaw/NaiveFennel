// automatically generated by the FlatBuffers compiler, do not modify

package com.bushpath.nfennel.flatbuffers;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class WriterOpenRequest extends Table {
  public static WriterOpenRequest getRootAsWriterOpenRequest(ByteBuffer _bb) { return getRootAsWriterOpenRequest(_bb, new WriterOpenRequest()); }
  public static WriterOpenRequest getRootAsWriterOpenRequest(ByteBuffer _bb, WriterOpenRequest obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public WriterOpenRequest __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String filename() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer filenameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer filenameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public String features(int j) { int o = __offset(6); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int featuresLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }

  public static int createWriterOpenRequest(FlatBufferBuilder builder,
      int filenameOffset,
      int featuresOffset) {
    builder.startObject(2);
    WriterOpenRequest.addFeatures(builder, featuresOffset);
    WriterOpenRequest.addFilename(builder, filenameOffset);
    return WriterOpenRequest.endWriterOpenRequest(builder);
  }

  public static void startWriterOpenRequest(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addFilename(FlatBufferBuilder builder, int filenameOffset) { builder.addOffset(0, filenameOffset, 0); }
  public static void addFeatures(FlatBufferBuilder builder, int featuresOffset) { builder.addOffset(1, featuresOffset, 0); }
  public static int createFeaturesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startFeaturesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endWriterOpenRequest(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

