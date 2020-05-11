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
import org.apache.nifi.annotation.lifecycle.OnStopped;
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

import java.util.Comparator;
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
import com.github.cliftonlabs.json_simple.Jsoner;

@Tags({ "json, timestamp, automation, calc, stream" })
@CapabilityDescription("Calculate timestamp difference between PLC signals in a stream")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class TimediffProcessor extends AbstractProcessor {

    public static final PropertyDescriptor Signal_desc_name = new PropertyDescriptor.Builder().name("signal_desc_name")
            .displayName("Signal name descriptor").description("Signal field in json").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Signal_desc_val = new PropertyDescriptor.Builder().name("signal_desc_val")
            .displayName("Signal value descriptor").description("Value field in json").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Signal_desc_timestamp = new PropertyDescriptor.Builder()
            .name("signal_desc_timestamp").displayName("Signal timestamp descriptor")
            .description("Timestamp field in json").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Signal_desc_timediff_name = new PropertyDescriptor.Builder()
            .name("signal_desc_timediff_name").displayName("Output timediff name descriptor")
            .description("Timediff field in output json").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor Signal_desc_timediff_val = new PropertyDescriptor.Builder()
            .name("signal_desc_timediff_val").displayName("Output timediff value descriptor")
            .description("Timediff field in output json").expressionLanguageSupported(true).required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor First_signal_name = new PropertyDescriptor.Builder()
            .name("first_signal_name").displayName("First signal name").description("Name of the first signal")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor First_signal_val = new PropertyDescriptor.Builder().name("first_signal_val")
            .displayName("First signal value").description("Value of the first signal").required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).expressionLanguageSupported(true).build();

    public static final PropertyDescriptor Second_signal_name = new PropertyDescriptor.Builder()
            .name("second_signal_name").displayName("Second signal name").description("Name of the second signal")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor Second_signal_val = new PropertyDescriptor.Builder()
            .name("second_signal_val").displayName("Second signal value").description("Value of the second signal")
            .required(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor Buffer_size = new PropertyDescriptor.Builder().name("buffer_size")
            .displayName("Buffer size").description("Buffer size").required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).expressionLanguageSupported(false)
            .defaultValue("5").build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success")
            .description("New flowfile in form of String with found/calculated time difference").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
            .description("Original flowfile").build();
    public static final Relationship FILTERED = new Relationship.Builder().name("filtered")
            .description("Filtered by name and value flowfiles").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private List<JsonObject> messagesJsonArray = new ArrayList<JsonObject>();

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
        descriptors.add(Buffer_size);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(FILTERED);
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

    @OnStopped
    public void onStopped(final ProcessContext context) {
        messagesJsonArray.clear();
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

        String prop_buffer_size = context.getProperty(Buffer_size).getValue();
        Integer buffer_size = Integer.parseInt(prop_buffer_size);

        final AtomicReference<Boolean> message_found = new AtomicReference<>();
        message_found.set(false);

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {

                    // Skopiownie przychodzącego stringa
                    //
                    String json = IOUtils.toString(in);

                    // Parsowanie na JsonObject
                    //
                    JsonObject parsedJson = (JsonObject) Jsoner.deserialize(json);

                    // Odczytanie pól z JSONa
                    //
                    String name = (String) parsedJson.get(prop_signal_desc_name);
                    Boolean val = (Boolean) parsedJson.get(prop_signal_desc_val);
                    // BigDecimal time = (BigDecimal) parsedJson.get(prop_signal_desc_timestamp);

                    // Jeżeli są to interesujące nas sygnały to dodajemy do listy
                    //
                    if ((name.equals(prop_first_signal_name) && val.equals(first_signal_val))
                            || (name.equals(prop_second_signal_name) && val.equals(second_signal_val))) {

                        // Dodanie do tablicy
                        //
                        messagesJsonArray.add(parsedJson);
                        message_found.set(true);

                        // Sortowanie
                        //
                        if (messagesJsonArray.size() > 1) {
                            Collections.sort(messagesJsonArray, new Comparator<JsonObject>() {
                                // You can change "Name" with "ID" if you want to sort by ID
                                private final String KEY_NAME = prop_signal_desc_timestamp;

                                @Override
                                public int compare(JsonObject a, JsonObject b) {
                                    BigDecimal t1 = (BigDecimal) a.get(KEY_NAME);
                                    BigDecimal t2 = (BigDecimal) b.get(KEY_NAME);
                                    if (t1.compareTo(BigDecimal.ZERO) > 0 && t2.compareTo(BigDecimal.ZERO) > 0) {
                                        return t1.compareTo(t2);
                                    } else {
                                        return 0;
                                    }
                                }
                            });
                        }
                    }

                    // Sprawdzamy czy jest już wystarczająca ilośc elementów w tablicy
                    //
                    if (messagesJsonArray.size() >= buffer_size) {

                        // JsonObject json1, json2;
                        Boolean found1, found2;
                        found1 = false;
                        found2 = false;
                        BigDecimal time1, time2;
                        time1 = new BigDecimal(0);
                        time2 = new BigDecimal(0);
                        JsonObject json1 = new JsonObject();
                        JsonObject json2 = new JsonObject();

                        // Sprawdzamy czy zdefiniowano drugi parametr
                        // Odczytujemy dane z tablicy
                        //
                        if (prop_second_signal_name != null && prop_second_signal_name != "") {

                            // 2 parametry
                            //

                            // Szukamy pierwszego w tablicy wystapienia sygnałów w tablicy
                            //
                            // for (JsonObject tab : messagesJsonArray) {
                            for (int i = 0; i < messagesJsonArray.size() - 1; i++) {
                                JsonObject obj1 = messagesJsonArray.get(i);
                                String name1, name2;
                                Boolean val1, val2;
                                name1 = (String) obj1.get(prop_signal_desc_name);
                                val1 = (Boolean) obj1.get(prop_signal_desc_val);
                                time1 = (BigDecimal) obj1.get(prop_signal_desc_timestamp);
                                if (name1.equals(prop_first_signal_name) && val1 == first_signal_val) {
                                    found1 = true;
                                    json1 = obj1;
                                    JsonObject obj2 = messagesJsonArray.get(i + 1);
                                    name2 = (String) obj2.get(prop_signal_desc_name);
                                    val2 = (Boolean) obj2.get(prop_signal_desc_val);
                                    time2 = (BigDecimal) obj2.get(prop_signal_desc_timestamp);
                                    if (name2.equals(prop_second_signal_name) && val2 == second_signal_val) {
                                        found2 = true;
                                        json2 = obj2;
                                        break;
                                    }
                                }
                            }
                            // for (JsonObject tab : messagesJsonArray) {
                            // name2 = (String) tab.get(prop_signal_desc_name);
                            // val2 = (Boolean) tab.get(prop_signal_desc_val);
                            // time2 = (BigDecimal) tab.get(prop_signal_desc_timestamp);
                            // json2 = tab;
                            // if (name2.equals(prop_second_signal_name) && val2 == second_signal_val) {
                            // found2 = true;
                            // break;
                            // }
                            // }
                        } else {
                            // 1 parametr
                            //

                            // String name1, name2;
                            // Boolean val1, val2;

                            // Pierwsza wartość
                            //
                            // name1 = (String) messagesJsonArray.get(0).get(prop_signal_desc_name);
                            // val1 = (Boolean) messagesJsonArray.get(0).get(prop_signal_desc_val);
                            time1 = (BigDecimal) messagesJsonArray.get(0).get(prop_signal_desc_timestamp);
                            json1 = messagesJsonArray.get(0);
                            found1 = true;

                            // Druga wartość
                            //
                            // name2 = (String) messagesJsonArray.get(1).get(prop_signal_desc_name);
                            // val2 = (Boolean) messagesJsonArray.get(1).get(prop_signal_desc_val);
                            time2 = (BigDecimal) messagesJsonArray.get(1).get(prop_signal_desc_timestamp);
                            json2 = messagesJsonArray.get(1);
                            found2 = true;
                        }

                        // Sprawdzamy czy znaleziono te dwa wpisy i czy wartości są większe od zera
                        //
                        if (found1 && found2 && time1.compareTo(BigDecimal.ZERO) > 0
                                && time2.compareTo(BigDecimal.ZERO) > 0) {

                            // Obliczamy różnicę
                            //
                            BigDecimal outval = time2.subtract(time1);

                            // Tworzymy nowego JSONa
                            //
                            final JsonObject newJson = new JsonObject();

                            // Dodajemy pola do JSONa
                            if (prop_signal_desc_timediff_name != null && prop_signal_desc_timediff_name != "") {
                                newJson.put(prop_signal_desc_name, prop_signal_desc_timediff_name);
                            } else {
                                newJson.put(prop_signal_desc_name, prop_first_signal_name + "(" + prop_first_signal_val
                                        + ")," + prop_second_signal_name + "(" + prop_second_signal_val + ")");
                            }

                            // Dodajemy obliczoną wartość
                            //
                            newJson.put(prop_signal_desc_timestamp, time2);
                            if (prop_signal_desc_timediff_val != null && prop_signal_desc_timediff_val != "") {
                                newJson.put(prop_signal_desc_timediff_val, outval);
                            } else {
                                newJson.put(prop_signal_desc_val, outval);
                            }

                            // Przekształcamy JSONa na Stringa
                            //
                            String outstring = newJson.toJson();

                            outstring += "\n"; // dodajemy znak końca linii (dla sparka)

                            // // TEST
                            // // pokazanie całej tablicy
                            // outstring += "\n";
                            // for (JsonObject tab : messagesJsonArray) {
                            // outstring += "[" + tab.toJson() + "]\n";
                            // }
                            // outstring += "\n";

                            // Zapisujemy do AtomicReference
                            value.set(outstring);

                            // Usunięcie użytych JSONów
                            messagesJsonArray.remove(json1);
                            messagesJsonArray.remove(json2);

                        }
                    }

                    // Zabezpieczenie - jeżeli mamy dwa razy tyle elementów w tablicy to ją czyścimy
                    //
                    if (messagesJsonArray.size() > buffer_size * 2) {
                        // Wyczyszczenie tablicy
                        messagesJsonArray.clear();
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string. " + ex.toString());
                }
            }
        });

        // Wysłanie wyniku do odpowiedniego endpointu
        //
        String results = value.get();
        Boolean results2 = message_found.get();

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
        } else if (results2) {
            session.transfer(flowfile, FILTERED);
        } else {
            session.transfer(flowfile, FAILURE);
        }

    }
}
