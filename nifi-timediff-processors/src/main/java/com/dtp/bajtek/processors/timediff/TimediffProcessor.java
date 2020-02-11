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

import org.json.simple.JSONObject;
import org.json.simple.parser.*;

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

    public static final PropertyDescriptor First_signal_name = new PropertyDescriptor.Builder()
            .name("first_signal_name").displayName("First signal name").description("Name of the first signal")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor First_signal_val = new PropertyDescriptor.Builder().name("first_signal_val")
            .displayName("First signal value").description("Value of the first signal").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor Second_signal_name = new PropertyDescriptor.Builder()
            .name("second_signal_name").displayName("Second signal name").description("Name of the second signal")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor Second_signal_val = new PropertyDescriptor.Builder()
            .name("second_signal_val").displayName("Second signal value").description("Value of the second signal")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("New flowfile in form of String with found/calculated time difference").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("Original flowfile").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public Long timestamp = 0L;
    public Long time0 = 0L;
    public Long time1 = 0L;
    public Boolean calc = false;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(Signal_desc_name);
        descriptors.add(Signal_desc_val);
        descriptors.add(Signal_desc_timestamp);
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
                    Object obj = new JSONParser().parse(json);
                    JSONObject parsedJson = (JSONObject) obj;

                    String name = (String) parsedJson.get(prop_signal_desc_name);
                    Boolean val = (Boolean) parsedJson.get(prop_signal_desc_val);
                    Long time = (Long) parsedJson.get(prop_signal_desc_timestamp);

                    if (name.equals(prop_first_signal_name) && val == first_signal_val) {
                        if (time != time0 && time1 != time) {
                            timestamp = time;
                            calc = true;
                        }
                        time1 = time;
                    }
                    if (name.equals(prop_second_signal_name) && val == second_signal_val) {
                        if (time != time1 && time != time0 && calc) {
                            Long outval = time - timestamp;
                            if (outval > 0 && outval < 10000) { // DODAĆ PROPERITIES MIN I MAX !!!
                                String outstring = "Timediff " + prop_second_signal_name + "[" + time + "] - "
                                        + prop_first_signal_name + "[" + timestamp + "] = " + outval.toString();
                                value.set(outstring);
                            }
                        }
                        calc = false;
                        time0 = time;
                    }

                    // String dane_in = name + " " + val + " " + time;
                    // String outstring = json;

                    // if (name.equals(prop_first_signal_name) && val == first_signal_val &&
                    // timestamp == 0L) {
                    // timestamp = time;
                    // outstring += " *** 1 *** ";
                    // }
                    // if (name.equals(prop_second_signal_name) && val == second_signal_val) {
                    // Long outval = time - timestamp;
                    // outstring = "Timediff " + prop_second_signal_name + "[" + second_signal_val +
                    // "] - "
                    // + prop_first_signal_name + "[" + first_signal_val + "] = " +
                    // outval.toString();
                    // outstring += " *** 2 *** ";
                    // timestamp = 0L;
                    // }

                    // value.set(dane_in + " " + outstring);

                    // String json = IOUtils.toString(in);
                    // value.set(json + " modified");

                    // value.set("Signal [" + prop_signal_desc_name + "] " + name_id + ". Looking
                    // for signals " + prop_first_signal_name + ", " + prop_second_signal_name);

                    // if (name_id.equals(prop_first_signal_name)) {
                    // value.set("Value " + val_id.toString() + " on signal " +
                    // prop_first_signal_name + " detected. Looking for value " +
                    // prop_first_signal_val.toString());
                    // }
                    // if (name_id.equals(prop_second_signal_name)) {
                    // value.set("Value " + val_id.toString() + " on signal " +
                    // prop_second_signal_name + " detected. Looking for value " +
                    // prop_second_signal_val.toString());
                    // }

                    // value.set("Value " + val.toString() + " on signal " + name + " on time " +
                    // time.toString() + " detected. Looking for " + prop_first_signal_name + "=" +
                    // first_signal_val);

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
