package org.apache.solr.common;

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

public class TestSpeedControllers {
  
  public static interface Interface {
    
    int zkClusterInfoMinUpdateInterval();
    
    int zkControllerSleepFactor();

    boolean fieldTypePluginLoaderUseFieldTypeCache();
    
    boolean zkCmdExecutorRetry();
    
  }
  
  public static class DefaultImplementation implements Interface {
    @Override
    public int zkClusterInfoMinUpdateInterval() {
      return 1500;
    }
    
    @Override
    public int zkControllerSleepFactor() {
      return 1000;
    }
  
    @Override
    public boolean fieldTypePluginLoaderUseFieldTypeCache() {
      return false;
    }
    
    @Override
    public boolean zkCmdExecutorRetry() {
      return true;
    }
  }
  
  private static Interface currentImplementation = new DefaultImplementation();
  
  public static Interface setImplementation(Interface newImpl) {
    Interface oldImpl = currentImplementation;
    currentImplementation = newImpl;
    return oldImpl;
  }
  
  public static int zkClusterInfoMinUpdateInterval() {
    return currentImplementation.zkClusterInfoMinUpdateInterval();
  }

  public static int zkControllerSleepFactor() {
    return currentImplementation.zkControllerSleepFactor();
  }

  public static boolean fieldTypePluginLoaderUseFieldTypeCache() {
    return currentImplementation.fieldTypePluginLoaderUseFieldTypeCache();
  }
  
  public static boolean zkCmdExecutorRetry() {
    return currentImplementation.zkCmdExecutorRetry();
  }

}
