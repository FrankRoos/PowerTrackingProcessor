<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

## Example

<p align="center"> 
    <img src="icon.png" width="150px;" class="pe-image-documentation"/>
</p>

***

## Description
This processor computes power per waiting time or hourly power  based on given instantaneous powers/timestamps values that are transmitted as fields from events.
Convert Instantaneous Power to Hourly Power.

***

## Required input
Input event requires to have power and timestamp values.

***

## Configuration
### Instantaneous Power
The field containing the power value as a double in Kilowatt (kW).
### Timestamp
The field containing the time value (in millisecond) at which the power was taken.
### Waiting Time
The field containing the period or time value (in minute) after which an output will be computed.

***

## Output
The Power Tracking processor appends the calculated power as a double in Kilowatt-hour (kWh).
