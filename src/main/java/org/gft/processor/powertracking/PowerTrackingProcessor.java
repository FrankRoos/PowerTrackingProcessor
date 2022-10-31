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

package org.gft.processor.powertracking;

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


public class PowerTrackingProcessor extends StreamPipesDataProcessor {
    private String input_power_value;
    private String input_timestamp_value;
    private static final String INPUT_VALUE = "value";
    private static final String TIMESTAMP_VALUE = "timestamp_value";
    private static final String WAITING_TIME = "time_range";

    List<Double> powersListForHourlyBasedComputation = new ArrayList<>();
    List<Integer> timestampsListForHourlyBasedComputation = new ArrayList<>();
    List<Double> powersListForWaitingTimeBasedComputation = new ArrayList<>();
    List<Integer> timestampsListForWaitingTimeBasedComputation = new ArrayList<>();
    private Integer hourlytime_start =0;
    private Integer waiting_time;
    private Integer waitingtime_start =0;

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.gft.processor.powertracking","TimeTracking", "Convert Instantaneous Power to Hourly Power")
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .category(DataProcessorType.AGGREGATE)
                .requiredStream(StreamRequirementsBuilder.create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                        .build())
                .requiredIntegerParameter(Labels.withId(WAITING_TIME))
                //.outputStrategy(OutputStrategies.append(PrimitivePropertyBuilder.create(Datatypes.Double, "outputValue").build()))
                .outputStrategy(OutputStrategies.custom())
                .build();
    }


    @Override
    public void onInvocation(ProcessorParams parameters, SpOutputCollector out, EventProcessorRuntimeContext ctx) throws SpRuntimeException  {
        this.input_power_value = parameters.extractor().mappingPropertyValue(INPUT_VALUE);
        this.input_timestamp_value = parameters.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
        this.waiting_time = parameters.extractor().singleValueParameter(WAITING_TIME, Integer.class);
    }

    @Override
    public void onEvent(Event event,SpOutputCollector out){
        double power_hourly = 0.0;
        double power_waitingtime = 0.0;
        int waiting_time = this.waiting_time*60*1000;

        //recovery input value
        Double value = event.getFieldBySelector(this.input_power_value).getAsPrimitive().getAsDouble();
        System.out.println("value: " + value);

        //recovery timestamp value
        Integer timestamp = event.getFieldBySelector(this.input_timestamp_value).getAsPrimitive().getAsInt();

       if(((timestamp - waitingtime_start >= waiting_time) || (timestamp - hourlytime_start >= 3600000)) && hourlytime_start != 0){

            if(timestamp - waitingtime_start >= waiting_time){
                // reset the start time
                waitingtime_start = timestamp;
                // add value to the lists
                powersListForWaitingTimeBasedComputation.add(value);
                timestampsListForWaitingTimeBasedComputation.add(timestamp);
                //perform operations to obtain waiting time power from instantaneous powers
                power_waitingtime = powerToEnergy(powersListForWaitingTimeBasedComputation, timestampsListForWaitingTimeBasedComputation);
                // Remove all elements from the Lists
                powersListForWaitingTimeBasedComputation.clear();
                timestampsListForWaitingTimeBasedComputation.clear();
            }

            if (timestamp - hourlytime_start >= 3600000) {
                // reset the start time
                hourlytime_start =timestamp;
                // add value to the lists
                powersListForHourlyBasedComputation.add(value);
                timestampsListForHourlyBasedComputation.add(timestamp);
                //perform operations to obtain hourly power from instantaneous powers
                power_hourly = powerToEnergy(powersListForHourlyBasedComputation, timestampsListForHourlyBasedComputation);
                // Remove all elements from the Lists
                powersListForHourlyBasedComputation.clear();
                timestampsListForHourlyBasedComputation.clear();
            }

        }else {
               if (hourlytime_start == 0){
                   // set the first timestamp
                   hourlytime_start = timestamp;
                   waitingtime_start = timestamp;
               }
               // add value to the lists
               powersListForHourlyBasedComputation.add(value);
               timestampsListForHourlyBasedComputation.add(timestamp);
               powersListForWaitingTimeBasedComputation.add(value);
               timestampsListForWaitingTimeBasedComputation.add(timestamp);
        }

        if(power_waitingtime != 0.0){
            System.out.println("======= OUTPUT VALUE ============" + power_waitingtime);
            event.addField("Power per Waiting Time", power_waitingtime);
            out.collect(event);
        }

        if(power_hourly != 0.0){
            System.out.println("======= OUTPUT VALUE ============" + power_hourly);
            event.addField("Hourly Power", power_hourly);
            out.collect(event);
        }

    }

    public double powerToEnergy(List<Double> powers, List<Integer> timestamps) {
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