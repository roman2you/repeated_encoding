package com.icongtai.zebra.encoding.impl.deltaprefixnumber;


import com.google.common.base.Preconditions;
import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesWriter;

/**
 * Write prefix lengths using delta encoding, followed by suffixes with Delta length byte arrays
 * This encoder for integer， 前缀一样，比如手机号码，gps等
 * <pre>
 *   {@code
 *   delta-prefixInt : rle prefix.toInt() , delta suffixes.toInt()
 *   } 
 * </pre>
 *
 */
public class DeltaPrefixIntegerWriter extends ValuesWriter {

  private ValuesWriter prefixWriter;
  private ValuesWriter suffixWriter;
  private int suffixLength;

  public DeltaPrefixIntegerWriter(int maxPrefix, int minPrefix,  int suffixLength, int initialCapacity) {
    Preconditions.checkArgument(maxPrefix >= minPrefix, "maxPrefix should > minPrefix");
    this.suffixLength = suffixLength;
    this.prefixWriter = new RunLengthBitPackingHybridValuesWriter(maxPrefix, minPrefix, initialCapacity);
    this.suffixWriter = new DeltaIntBitPackingValuesWriter(32, 1, initialCapacity);
  }

  public DeltaPrefixIntegerWriter(long maxPrefix, long minPrefix,  int suffixLength, int initialCapacity) {
    Preconditions.checkArgument(maxPrefix > minPrefix, "max > min");
    this.suffixLength = suffixLength;

    this.prefixWriter = new RunLengthBitPackingHybridValuesWriter(maxPrefix, minPrefix, initialCapacity);
    this.suffixWriter = new DeltaIntBitPackingValuesWriter(32, 1, initialCapacity);
  }

  @Override
  public long getBufferedSize() {
    return prefixWriter.getBufferedSize() + suffixWriter.getBufferedSize();
  }

  @Override
  public BytesInput getBytes() {
    return BytesInput.concat(prefixWriter.getBytes(), suffixWriter.getBytes());
  }

  @Override
  public EncodeType getEncoding() {
    return EncodeType.delta_prefix_int;
  }

  @Override
  public void reset() {
    prefixWriter.reset();
    suffixWriter.reset();
  }

  @Override
  public long getAllocatedSize() {
    return prefixWriter.getAllocatedSize() + suffixWriter.getAllocatedSize();
  }

  @Override
  public String memUsageString(String prefix) {
    prefix = prefixWriter.memUsageString(prefix);
    return suffixWriter.memUsageString(prefix + "  DELTA_GPS");
  }

  @Override
  public void writeInteger(int v) {
    String vString = String.valueOf(v);
    if(vString.length() < suffixLength) {
      throw new ZebraEncodingException("v.length < prefixLength");
    }
    int prefix = Integer.parseInt(vString.substring(0, vString.length() - suffixLength));;
    int suffix = Integer.parseInt(vString.substring(vString.length() - suffixLength));;
    prefixWriter.writeInteger(prefix);
    suffixWriter.writeInteger(suffix);
  }

  @Override
  public void writeLong(long v) {
    String vString = String.valueOf(v);
    if(vString.length() < suffixLength) {
      throw new ZebraEncodingException("v.length < prefixLength");
    }
    int prefix = Integer.parseInt(vString.substring(0, vString.length() - suffixLength));
    int suffix = Integer.parseInt(vString.substring(vString.length() - suffixLength));

    prefixWriter.writeInteger(prefix);
    suffixWriter.writeInteger(suffix);
  }
}
