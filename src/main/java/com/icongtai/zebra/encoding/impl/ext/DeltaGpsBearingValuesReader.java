package com.icongtai.zebra.encoding.impl.ext;

import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;

import java.io.IOException;


/**
 */
class DeltaGpsBearingValuesReader  extends DeltaIntBitPackingValuesReader {
    @Override
    protected void writeDeltaValues(int valueUnpacked) {
        for (int j = valuesBuffered-valueUnpacked; j < valuesBuffered; j++) {
            int index = j;

            int previousValue = valuesBuffer[index - 1];

            int deltaValue =  minDeltaInCurrentBlock + valuesBuffer[index];

            int currentValue = deltaValue + previousValue;

            if(currentValue < 0) {
                currentValue = 3600 + currentValue;
            } else if(currentValue > 3600) {
                currentValue = currentValue - 3600;
            }

            valuesBuffer[index] = currentValue;

        }
    }

    public static void main(String[] args) throws IOException{
        DeltaGpsBearingValuesWriter writer = new DeltaGpsBearingValuesWriter(32);
        writer.writeInteger(10);
        writer.writeInteger(3555);
        writer.writeInteger(3550);
        writer.writeInteger(3450);
        writer.writeInteger(3451);
        writer.writeInteger(3452);
        writer.writeInteger(3453);
        writer.writeInteger(3454);
        writer.writeInteger(3455);
        writer.writeInteger(1);
        writer.writeInteger(15);
        writer.writeInteger(25);

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);

        DeltaGpsBearingValuesReader reader = new DeltaGpsBearingValuesReader();
        reader.init(1, bytes, 0);
        for(int i = 0; i < reader.getTotalValueCount(); i++) {
            System.out.println(reader.readInteger());
        }
    }
}
