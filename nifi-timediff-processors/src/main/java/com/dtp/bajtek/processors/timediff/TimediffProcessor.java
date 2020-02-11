/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtp.bajtek.processors.timediff;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import org.apache.commons.io.IOUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;

import com.github.cliftonlabs.json_simple.JsonObject;
// import com.github.cliftonlabs.json_simple.Jsonable;
import com.github.cliftonlabs.json_simple.Jsoner;
// import org.dozer.DozerBeanMapper;
// import org.dozer.Mapper;

@Tags({ "json, timestamp, automation, calc, stream" })
@CapabilityDescription("Calculate timestamp difference between PLC signals in a stream")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class TimediffProcessor extends AbstractProcessor {

    public static final PropertyDescriptor Signal_desc_name = new PropertyDescriptor.Builder().name("signal_desc_name")
            .displayName("Signal name descriptor").description("Signal field in json").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor Signal_desc_val = new PropertyDescriptor.Builder().name("signal_desc_val")
            .displayName("Signal value descriptor").description("Value field in json").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor Signal_desc_timestamp = new PropertyDescriptor.Builder()
            .name("signal_desc_timestamp").displayName("Signal timestamp descriptor")
            .description("Timestamp field in json").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor Signal_desc_timediff_name = new PropertyDescriptor.Builder()
            .name("signal_desc_timediff_name").displayName("Output timediff name descriptor")
            .description("Timediff field in output json").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor Signal_desc_timediff_val = new PropertyDescriptor.Builder()
            .name("signal_desc_timediff_val").displayName("Output timediff value descriptor")
            .description("Timediff field in output json").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor First_signal_name = new PropertyDescriptor.Builder()
            .name("first_signal_name").displayName("First signal name").description("Name of the first signal")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor First_signal_val = new PropertyDescriptor.Builder().name("first_signal_val")
            .displayName("First signal value").description("Value of the first signal").required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor Second_signal_name = new PropertyDescriptor.Builder()
            .name("second_signal_name").displayName("Second signal name").description("Name of the second signal")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor Second_signal_val = new PropertyDescriptor.Builder()
            .name("second_signal_val").displayName("Second signal value").description("Value of the second signal")
            .required(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("New flowfile in form of String with found/calculated time difference").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("Original flowfile").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public BigDecimal timestamp = new BigDecimal(0);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(Signal_desc_name);
        descriptors.add(Signal_desc_val);
        descriptors.add(Signal_desc_timestamp);
        descriptors.add(Signal_desc_timediff_name);
        descriptors.add(Signal_desc_timediff_val);
        descriptors.add(First_signal_name);
        descriptors.add(First_signal_val);
        descriptors.add(Second_signal_name);
        descriptors.add(Second_signal_val);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        if (flowfile == null) {
            return;
        }

        String prop_signal_desc_name = context.getProperty(Signal_desc_name).getValue();
        String prop_signal_desc_val = context.getProperty(Signal_desc_val).getValue();
        String prop_signal_desc_timestamp = context.getProperty(Signal_desc_timestamp).getValue();
        String prop_signal_desc_timediff_name = context.getProperty(Signal_desc_timediff_name).getValue();
        String prop_signal_desc_timediff_val = context.getProperty(Signal_desc_timediff_val).getValue();

        String prop_first_signal_name = context.getProperty(First_signal_name).getValue();
        String prop_first_signal_val = context.getProperty(First_signal_val).getValue();
        Boolean first_signal_val = Boolean.parseBoolean(prop_first_signal_val);

        String prop_second_signal_name = context.getProperty(Second_signal_name).getValue();
        String prop_second_signal_val = context.getProperty(Second_signal_val).getValue();
        Boolean second_signal_val = Boolean.parseBoolean(prop_second_signal_val);

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {

                    String json = IOUtils.toString(in);

                    // Object obj = new JSONParser().parse(json);
                    JsonObject parsedJson = (JsonObject) Jsoner.deserialize(json);

                    String name = (String) parsedJson.get(prop_signal_desc_name);
                    Boolean val = (Boolean) parsedJson.get(prop_signal_desc_val);
                    BigDecimal time = (BigDecimal) parsedJson.get(prop_signal_desc_timestamp);

                    // Sprawdzamy czy zdefiniowano drugi parametr
                    if (prop_second_signal_name != null && prop_second_signal_name != "") {
                        // 2 parametry
                        if (name.equals(prop_first_signal_name) && val == first_signal_val) {
                            timestamp = time;
                        }
                        if (name.equals(prop_second_signal_name) && val == second_signal_val && timestamp != time
                                && timestamp.compareTo(BigDecimal.ZERO) > 0 && time.compareTo(BigDecimal.ZERO) > 0) {
                            BigDecimal outval = time.subtract(timestamp);
                            if (outval.compareTo(BigDecimal.ZERO) > 0) {

                                final JsonObject newJson = new JsonObject();

                                if (prop_signal_desc_timediff_name != null && prop_signal_desc_timediff_name != "") {
                                    newJson.put(prop_signal_desc_name, prop_signal_desc_timediff_name);
                                } else {
                                    newJson.put(prop_signal_desc_name,
                                            prop_first_signal_name + "(" + prop_first_signal_val + "),"
                                                    + prop_second_signal_name + "(" + prop_second_signal_val + ")");
                                }

                                newJson.put(prop_signal_desc_timestamp, time);

                                if (prop_signal_desc_timediff_name != null && prop_signal_desc_timediff_name != "") {
                                    newJson.put(prop_signal_desc_timediff_val, outval);
                                } else {
                                    newJson.put(prop_signal_desc_val, outval);
                                }

                                String outstring = newJson.toJson();

                                // String outstring = "Timediff " + prop_second_signal_name + "[" + time + "] -
                                // "
                                // + prop_first_signal_name + "[" + timestamp + "] = " + outval.toString();

                                value.set(outstring);
                            }
                        }
                    } else {
                        // 1 parametr
                        if (name.equals(prop_first_signal_name) && val == first_signal_val) {
                            if (timestamp != time && timestamp.compareTo(BigDecimal.ZERO) > 0  && time.compareTo(BigDecimal.ZERO) > 0) {
                                BigDecimal outval = time.subtract(timestamp);
                                if (outval.compareTo(BigDecimal.ZERO) > 0) {
                                    final JsonObject newJson = new JsonObject();
                                    if (prop_signal_desc_timediff_name != null
                                            && prop_signal_desc_timediff_name != "") {
                                        newJson.put(prop_signal_desc_name, prop_signal_desc_timediff_name);
                                    } else {
                                        newJson.put(prop_signal_desc_name,
                                                prop_first_signal_name + "(" + prop_first_signal_val + ")");
                                    }
                                    newJson.put(prop_signal_desc_timestamp, time);
                                    if (prop_signal_desc_timediff_name != null
                                            && prop_signal_desc_timediff_name != "") {
                                        newJson.put(prop_signal_desc_timediff_val, outval);
                                    } else {
                                        newJson.put(prop_signal_desc_val, outval);
                                    }
                                    String outstring = newJson.toJson();
                                    value.set(outstring);
                                }
                            }
                            timestamp = time;
                        }
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string. " + ex.toString());
                }
            }
        });

        String results = value.get();
        if (results != null && !results.isEmpty()) {

            // // Write the results to an attribute
            // String results = value.get();
            // if (results != null && !results.isEmpty()) {
            // flowfile = session.putAttribute(flowfile, "match", results);
            // }

            // To write the results back out ot flow file
            flowfile = session.write(flowfile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(results.getBytes());
                }
            });

            session.transfer(flowfile, SUCCESS);
        } else {
            session.transfer(flowfile, FAILURE);
        }

    }
}
