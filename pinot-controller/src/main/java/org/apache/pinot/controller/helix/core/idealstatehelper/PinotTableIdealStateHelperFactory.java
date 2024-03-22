/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.idealstatehelper;

import org.apache.pinot.controller.ControllerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableIdealStateHelperFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableIdealStateHelperFactory.class);
  private static PinotTableIdealStateHelper _instance = null;
  private static ControllerConf _controllerConf;

  private PinotTableIdealStateHelperFactory() {
  }

  public static void init(ControllerConf controllerConf) {
    _controllerConf = controllerConf;
  }

  public static PinotTableIdealStateHelper create() {
    if (_instance == null) {
      String pinotTableIdealstateHelperClassPath = _controllerConf.getPinotTableIdealstateHelperClass();
      try {
        _instance = (PinotTableIdealStateHelper) Class.forName(pinotTableIdealstateHelperClassPath).newInstance();
      } catch (Exception e) {
        LOGGER.error("PinotTableIdealStateHelper not found: {}", pinotTableIdealstateHelperClassPath);
        throw new RuntimeException(e);
      }
    }
    return _instance;
  }
}
