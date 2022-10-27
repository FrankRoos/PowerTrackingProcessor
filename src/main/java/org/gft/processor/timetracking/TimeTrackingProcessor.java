/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gft.processor.timetracking;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.PrimitivePropertyBuilder;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.EpRequirements;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.sdk.utils.Datatypes;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

import java.util.ArrayList;
import java.util.List;


public class TimeTrackingProcessor extends StreamPipesDataProcessor {

    private String input_value;
    private String timestamp_value;
    private static final String INPUT_VALUE = "value";
    private static final String TIMESTAMP_VALUE = "timestamp_value";

    List<Double> powerValueList = new ArrayList<>();
    List<Double> timestampValueList = new ArrayList<>();
    private Double first_timestamp=0.0;

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.gft.processor.timetracking")
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .category(DataProcessorType.AGGREGATE)
                .requiredStream(StreamRequirementsBuilder.create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                        .build())
                .outputStrategy(OutputStrategies.append(PrimitivePropertyBuilder.create(Datatypes.Double, "outputValue").build()))
                .build();
    }


    @Override
    public void onInvocation(ProcessorParams processorParams,
                             SpOutputCollector out,
                             EventProcessorRuntimeContext ctx) throws SpRuntimeException  {
        this.input_value = processorParams.extractor().mappingPropertyValue(INPUT_VALUE);
        this.timestamp_value = processorParams.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
    }

    @Override
    public void onEvent(Event event,SpOutputCollector out){
        double energy = 0.0;

        //recovery input value
        Double value = event.getFieldBySelector(this.input_value).getAsPrimitive().getAsDouble();
        System.out.println("value: " + value);

        //recovery timestamp value
        Double timestamp = event.getFieldBySelector(this.timestamp_value).getAsPrimitive().getAsDouble();
        System.out.println("timestampStr: " + timestamp);

        System.out.println("timestamp Diff: " + (timestamp - first_timestamp));

        if (first_timestamp == 0.0){
            System.out.println("******** first_timestamp == 0.0 ************");
            first_timestamp=timestamp;
            powerValueList.add(value);
            timestampValueList.add(timestamp);
        }else if (timestamp - first_timestamp >= 3600000){
            System.out.println("******** DIFF >= 3600000 ************");
            powerValueList.add(value);
            timestampValueList.add(timestamp);
            first_timestamp=timestamp;
            //perform operations to obtain energy/hourly power from instantaneous powers
            energy = powerToEnergy(powerValueList, timestampValueList);
            // Remove all elements from the Lists
            powerValueList.clear();
            timestampValueList.clear();
        }else {
            System.out.println("******** ELSE ************");
            powerValueList.add(value);
            timestampValueList.add(timestamp);
        }

        if(energy != 0.0){
            System.out.println("======= OUTPUT VALUE ============" + energy);
            event.addField("Energy", energy);
            out.collect(event);
        }
    }

    public double powerToEnergy(List<Double> powers, List<Double> timestamps) {
        double sum = 0.0;
        //perform Riemann approximations by trapezoids which is an approximation of the area
        // under the curve (which corresponds to the energy/hourly power) formed by the points
        // with coordinate power(ordinate) e timestamp(abscissa)
        for(int i = 0; i<powers.size()-1; i++){
            sum += ((powers.get(i)+powers.get(i+1))/2) * (timestamps.get(i+1) - timestamps.get(i)) ;
        }
        return sum;
    }

    @Override
    public void onDetach(){
    }

}