package io.xrex.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.text.DecimalFormat;

public class DoubleToStringSerializer extends JsonSerializer<Double> {
    private static DecimalFormat df1 = new DecimalFormat("##.########");

    public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(df1.format(value));
    }
}