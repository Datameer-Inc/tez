/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common.security;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class Master {

  private static final Logger LOG = LoggerFactory.getLogger(Master.class);
  public enum State {
    INITIALIZING, RUNNING;
  }

  public static String getMasterUserName(Configuration conf) {
    return conf.get(YarnConfiguration.RM_PRINCIPAL);
  }

  
  // This needs to go into YARN
  public static InetSocketAddress getMasterAddress(Configuration conf) {
    if (HAUtil.isHAEnabled(conf)) {
	  YarnConfiguration yarnConf = new YarnConfiguration(conf);
	  if (yarnConf.get(YarnConfiguration.RM_HA_ID) == null) {
	    String[] rmIds = yarnConf.getStrings(YarnConfiguration.RM_HA_IDS);
	    if (rmIds != null && rmIds.length > 0) {
	      // If RM_HA_ID is not configured, use the first one.
	      // Because any valid RM HA ID should work.
	      yarnConf.set(YarnConfiguration.RM_HA_ID, rmIds[0]);
	    } else {
	      LOG.warn("RM_HA_IDS is not configured when RM HA is enabled");
	    }
	  }
	  return yarnConf.getSocketAddr(
	      YarnConfiguration.RM_ADDRESS,
	      YarnConfiguration.DEFAULT_RM_ADDRESS,
	      YarnConfiguration.DEFAULT_RM_PORT);
	  } else {
	    return conf.getSocketAddr(
	        YarnConfiguration.RM_ADDRESS,
	        YarnConfiguration.DEFAULT_RM_ADDRESS,
	        YarnConfiguration.DEFAULT_RM_PORT);
	  }
  }
  
  public static String getMasterPrincipal(Configuration conf)
      throws IOException {
    String masterHostname = getMasterAddress(conf).getHostName();
    // get kerberos principal for use as delegation token renewer
    return SecurityUtil.getServerPrincipal(getMasterUserName(conf),
        masterHostname);
  }

}
